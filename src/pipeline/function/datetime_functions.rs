use chrono::{Duration, NaiveDate, Datelike};

use crate::pipeline::PiperError;

fn get_days_from_month(year: i32, month: u32) -> u32 {
    NaiveDate::from_ymd_opt(
        match month {
            12 => year + 1,
            _ => year,
        },
        match month {
            12 => 1,
            _ => month + 1,
        },
        1,
    )
    .unwrap()
    .signed_duration_since(NaiveDate::from_ymd_opt(year, month, 1).unwrap())
    .num_days() as u32
}

pub fn add_months(date: NaiveDate, num_months: u32) -> NaiveDate {
    let mut month = date.month() + num_months;
    let year = date.year() + (month / 12) as i32;
    month %= 12;
    let mut day = date.day();
    let max_days = get_days_from_month(year, month);
    day = if day > max_days { max_days } else { day };
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

pub fn add_days(date: String, days: i64) -> Result<String, PiperError> {
    let mut date = NaiveDate::parse_from_str(&date, "%Y-%m-%d")
        .map_err(|e| PiperError::InvalidValue(format!("Invalid date: {}", e)))?;
    date += Duration::days(days);
    Ok(date.format("%Y-%m-%d").to_string())
}
