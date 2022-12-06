use std::str::FromStr;

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;

use crate::pipeline::{value::IntoValue, PiperError, Value};

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

pub fn add_days(mut date: NaiveDate, days: i64) -> Result<NaiveDate, PiperError> {
    date += Duration::days(days);
    Ok(date)
}

pub fn quarter(date: NaiveDate) -> u32 {
    (date.month() - 1) / 3 + 1
}

pub fn from_utc_timestamp(dt: Value, tz: String) -> Result<NaiveDateTime, PiperError> {
    let dt = dt.get_datetime()?.naive_utc();
    let tz = Tz::from_str(&tz)
        .map_err(|e| PiperError::InvalidValue(format!("Invalid timezone: {}", e)))?;
    let local_dt = tz.from_local_datetime(&dt).unwrap().naive_utc();
    Ok(local_dt)
}

pub fn to_timestamp(arguments: Vec<Value>) -> Result<DateTime<Utc>, PiperError> {
    if arguments.len() > 3 {
        return Err(PiperError::ArityError(
            "timestamp".to_string(),
            arguments.len(),
        ));
    }

    let dt = arguments[0].get_string()?;
    let format = if arguments.len() > 1 {
        arguments[1].get_string()?
    } else {
        "%Y-%m-%d %H:%M:%S".into()
    };
    let tz = if arguments.len() > 2 {
        arguments[2].get_string()?
    } else {
        "UTC".into()
    };

    let ret = NaiveDateTime::parse_from_str(dt.as_ref(), format.as_ref())
        .map_err(|e| PiperError::InvalidValue(format!("Invalid datetime: {}", e)))?;

    let tz = Tz::from_str(&tz)
        .map_err(|e| PiperError::InvalidValue(format!("Invalid timezone: {}", e)))?;

    Ok(tz.from_local_datetime(&ret).unwrap().with_timezone(&Utc))
}

pub fn make_timestamp(arguments: Vec<Value>) -> Result<Value, PiperError> {
    if arguments.len() < 6 {
        return Err(PiperError::ArityError(
            "make_timestamp".to_string(),
            arguments.len(),
        ));
    }
    let y = arguments[0].get_int()?;
    let mon = arguments[1].get_int()?;
    let d = arguments[2].get_int()?;
    let h = arguments[3].get_int()?;
    let m = arguments[4].get_int()?;
    let s = arguments[5].get_int()?;
    let tz = if arguments.len() > 6 {
        arguments[6].get_string()?.to_string()
    } else {
        "UTC".to_string()
    };
    Ok(from_utc_timestamp(
        NaiveDate::from_ymd_opt(y, mon as u32, d as u32)
            .unwrap()
            .and_hms_opt(h as u32, m as u32, s as u32)
            .unwrap()
            .into_value(),
        tz,
    )
    .into_value())
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{function::datetime_functions::to_timestamp, value::IntoValue};

    use super::from_utc_timestamp;

    #[test]
    fn test_from_utc_timestamp() {
        let cst = "2022-03-04 13:00:00"
            .into_value()
            .get_datetime()
            .into_value();
        let gmt = "2022-03-04 05:00:00"
            .into_value()
            .get_datetime()
            .into_value();

        let dt = from_utc_timestamp(cst, "Asia/Shanghai".to_string()).into_value();
        assert_eq!(dt, gmt);
    }

    #[test]
    fn test_to_timestamp() {
        let gmt = "2022-03-04 05:00:00"
            .into_value()
            .get_datetime()
            .into_value();

        let dt = to_timestamp(vec![
            "2022/03/04 13:00".into_value(),
            "%Y/%-m/%-d %-H:%-M".into_value(),
            "Asia/Shanghai".into_value(),
        ])
        .into_value();
        assert_eq!(dt, gmt);

        let dt = to_timestamp(vec![
            "2022-03-04 05:00:00".into_value(),
        ])
        .into_value();
        assert_eq!(dt, gmt);
    }
}
