use chrono::{DateTime, Utc};
use cron::Schedule;
use std::str::FromStr;
use thiserror::Error;

/// A validated cron expression used by Rivet's persistent trigger subsystem.
///
/// Rivet accepts the familiar five-field form (`minute hour day month
/// weekday`) and the six/seven-field forms understood by the `cron` parser
/// (`second minute hour day month weekday [year]`). Five-field expressions are
/// normalized with a zero seconds field before evaluation. All occurrences
/// are evaluated in UTC until timezone-aware schedules are introduced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronExpression {
    expression: String,
    schedule: Schedule,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum CronExpressionError {
    #[error("cron expression cannot be empty")]
    Empty,
    #[error("cron expression must contain 5, 6, or 7 fields; found {0}")]
    InvalidFieldCount(usize),
    #[error("invalid cron expression: {0}")]
    InvalidExpression(String),
    #[error("cron expression has no future occurrence")]
    NoFutureOccurrence,
}

impl CronExpression {
    pub fn parse(expression: &str) -> Result<Self, CronExpressionError> {
        let expression = expression.trim();
        if expression.is_empty() {
            return Err(CronExpressionError::Empty);
        }

        let normalized = normalize_expression(expression)?;
        let schedule = Schedule::from_str(&normalized)
            .map_err(|error| CronExpressionError::InvalidExpression(error.to_string()))?;
        Ok(Self {
            expression: expression.to_owned(),
            schedule,
        })
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }

    pub fn next_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>, CronExpressionError> {
        self.schedule
            .after(&after)
            .next()
            .ok_or(CronExpressionError::NoFutureOccurrence)
    }
}

impl FromStr for CronExpression {
    type Err = CronExpressionError;

    fn from_str(expression: &str) -> Result<Self, Self::Err> {
        Self::parse(expression)
    }
}

fn normalize_expression(expression: &str) -> Result<String, CronExpressionError> {
    if expression.starts_with('@') {
        return Ok(expression.to_owned());
    }

    let fields = expression.split_whitespace().count();
    match fields {
        5 => Ok(format!("0 {expression}")),
        6 | 7 => Ok(expression.to_owned()),
        count => Err(CronExpressionError::InvalidFieldCount(count)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn timestamp(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 13, hour, minute, second)
            .single()
            .expect("valid timestamp")
    }

    #[test]
    fn evaluates_familiar_five_field_cron_in_utc() {
        let expression = CronExpression::parse("*/5 * * * *").expect("cron");
        assert_eq!(
            expression.next_after(timestamp(12, 2, 0)).unwrap(),
            timestamp(12, 5, 0)
        );
    }

    #[test]
    fn accepts_six_and_seven_field_forms() {
        let six = CronExpression::parse("0 */15 * * * *").expect("six fields");
        assert_eq!(
            six.next_after(timestamp(12, 2, 0)).unwrap(),
            timestamp(12, 15, 0)
        );

        let seven = CronExpression::parse("0 0 9 * * MON 2026").expect("seven fields");
        assert_eq!(
            seven.next_after(timestamp(8, 0, 0)).unwrap(),
            Utc.with_ymd_and_hms(2026, 9, 14, 9, 0, 0)
                .single()
                .expect("next Monday")
        );
    }

    #[test]
    fn accepts_hourly_shorthand_and_rejects_malformed_input() {
        let hourly = CronExpression::parse("@hourly").expect("shorthand");
        assert_eq!(
            hourly.next_after(timestamp(12, 2, 0)).unwrap(),
            timestamp(13, 0, 0)
        );
        assert_eq!(
            CronExpression::parse("* * *").unwrap_err(),
            CronExpressionError::InvalidFieldCount(3)
        );
        assert!(matches!(
            CronExpression::parse("61 * * * *"),
            Err(CronExpressionError::InvalidExpression(_))
        ));
    }
}
