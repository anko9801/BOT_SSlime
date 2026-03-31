use chrono::{DateTime, Local, TimeZone};
use sqlx::MySqlPool;

use crate::{
    model::{
        api,
        db::{self, MessageRecord},
    },
    naive_to_local,
};

pub async fn get_latest_message(pool: &MySqlPool) -> anyhow::Result<Option<MessageRecord>> {
    let message = db::get_latest_message(pool).await?;
    Ok(message)
}

/// after から before の期間のメッセージを API から取得し、DB に保存する
///
/// Returns the count of fetched messages (they are saved to DB, not accumulated in memory).
async fn fetch_messages_as_match_as_possible_at_once<TzB, TzA>(
    pool: &MySqlPool,
    before: Option<&DateTime<TzB>>,
    after: Option<&DateTime<TzA>>,
    interval_ms: u64,
) -> anyhow::Result<FetchResult>
where
    TzB: TimeZone,
    TzB::Offset: std::fmt::Display,
    TzA: TimeZone,
    TzA::Offset: std::fmt::Display,
{
    let (limit, res_messages) = api::get_messages_with_time_section(0, before, after).await?;

    db::insert_messages(
        pool,
        &res_messages
            .iter()
            .map(MessageRecord::from)
            .collect::<Vec<MessageRecord>>(),
    )
    .await?;

    let last = res_messages.last().map(|m| MessageRecord::from(m));
    let mut count = res_messages.len();

    while count < limit {
        let (_, res_messages) =
            api::get_messages_with_time_section(count, before, after).await?;

        tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;

        db::insert_messages(
            pool,
            &res_messages
                .iter()
                .map(MessageRecord::from)
                .collect::<Vec<MessageRecord>>(),
        )
        .await?;

        count += res_messages.len();
    }

    if count > limit {
        count = limit;
    }

    Ok(FetchResult { count, last })
}

struct FetchResult {
    count: usize,
    last: Option<MessageRecord>,
}

/// ある時点より新しいメッセージすべてを最大 limit 件取得し、DB に保存する
pub async fn fetch_messages<Tz>(
    pool: &MySqlPool,
    limit: Option<usize>,
    after: Option<DateTime<Tz>>,
) -> anyhow::Result<()>
where
    Tz: TimeZone,
    Tz::Offset: std::fmt::Display,
{
    let result = fetch_messages_as_match_as_possible_at_once(
        pool,
        None::<&DateTime<Local>>,
        after.as_ref(),
        300,
    )
    .await?;

    if result.count == 0 {
        return Ok(());
    }

    let mut total_count = result.count;
    let mut last = result.last;

    loop {
        if let Some(limit) = limit {
            if total_count >= limit {
                break;
            }
        }
        let oldest = match &last {
            Some(m) => m,
            None => break,
        };
        let oldest_created_at_local = naive_to_local(oldest.created_at);

        let result = fetch_messages_as_match_as_possible_at_once(
            pool,
            Some(&oldest_created_at_local),
            after.as_ref(),
            300,
        )
        .await?;

        if result.count == 0 {
            break;
        }

        total_count += result.count;
        last = result.last;
    }

    Ok(())
}
