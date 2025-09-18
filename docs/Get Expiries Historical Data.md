Here’s the **exact content** from your uploaded PDF in **Markdown format**:

---

# Expired Historical Candle Data | Upstox Developer API

## Developer API - Expired Instruments - Get Expired Historical Candle Data

### Get Expired Historical Candle Data

This API extends the functionality of the existing **Get Historical Candle Data API** by allowing users to query data for expired contracts.

This API is particularly useful for traders and analysts who need to analyze past performance and trends of expired contracts.

It provides historical **Open, High, Low, Close (OHLC)** data for expired contracts, available across multiple time intervals including:
`1minute`, `3minute`, `5minute`, `15minute`, `30minute`, and `day`.

> **NOTE:**
>
> * Before using this API, utilize the **Get Expired Option Contracts API** or **Get Expired Future Contracts API** to obtain the `expired_instrument_key` for the expired contracts.
> * This key, required as a path parameter, is a combination of the standard instrument key and expiry date returned by the previously mentioned APIs.
> * For OHLC data of an **active contract**, use the **Historical Candle Data API**.

> **IMPORTANT:**
>
> * Expired Historical Candle Data is currently not available for the **MCX**.
> * This API is specifically for expired contracts that have passed their expiry date.

---

## Header Parameters

| Name   | Required | Type   | Description                                                                         |
| ------ | -------- | ------ | ----------------------------------------------------------------------------------- |
| Accept | true     | string | Defines the content format the client expects, should be set to `application/json`. |

---

## Path Parameters

| Name                     | Required | Type   | Description                                                                                                    |
| ------------------------ | -------- | ------ | -------------------------------------------------------------------------------------------------------------- |
| expired\_instrument\_key | true     | string | Unique identifier for the expired instrument. Combination of normal instrument key and expiry date.            |
| interval                 | true     | string | Time frame of the candles.<br>Possible values: `1minute`, `3minute`, `5minute`, `15minute`, `30minute`, `day`. |
| to\_date                 | true     | string | Ending date (inclusive) for the historical data range. Format: `YYYY-MM-DD`.                                   |
| from\_date               | true     | string | Starting date for the historical data range. Format: `YYYY-MM-DD`.                                             |

---

## Responses

* **200**

### Response Body

| Name         | Type   | Description                                                            |
| ------------ | ------ | ---------------------------------------------------------------------- |
| status       | string | Outcome of the request. Typically `success` for successful operations. |
| data         | object | Contains OHLC values for expired contracts across various timeframes.  |
| data.candles | array  | Array of candle data, each presented as an array of trading activity.  |

---

### Candle Array Format

Each `data.candles` entry contains the following values in sequence:

| Index | Field         | Type   | Description                                                            |
| ----- | ------------- | ------ | ---------------------------------------------------------------------- |
| \[0]  | Timestamp     | string | Start time of the candle’s timeframe.                                  |
| \[1]  | Open          | number | Opening price of the asset.                                            |
| \[2]  | High          | number | Highest price traded during the timeframe.                             |
| \[3]  | Low           | number | Lowest price traded during the timeframe.                              |
| \[4]  | Close         | number | Closing price of the asset.                                            |
| \[5]  | Volume        | number | Total amount of the asset traded during the timeframe.                 |
| \[6]  | Open Interest | number | Total number of outstanding derivative contracts (options or futures). |

---

### Example Response

```json
{
  "status": "success",
  "data": {
    "candles": [
      [
        "2022-11-30T00:00:00+05:30",
        125.35,
        126.8,
        122.1,
        123.45,
        1542678,
        184632
      ],
      [
        "2022-11-29T00:00:00+05:30",
        120.85,
        126.55,
        120.5,
        125.35,
        1876543,
        195732
      ]
    ]
  }
}
```

---

## Request

**Endpoint:**

```
GET  /expired-instruments/historical-candle/:expired_instrument_key/:interval/:to_date/:from_date
```

**cURL Example:**

```bash
curl -L -X GET 'https://api.upstox.com/v2/expired-instruments/historical-candle/:expired_instrument_key/:interval/:to_date/:from_date' \
-H 'Accept: application/json'
```

---

🔗 [More API Examples](https://upstox.com/developer/api-documentation/example-code/expired-instruments/get-expired-historical-candle-data)

---

Would you like me to **merge all four docs** (Expiries, Expired Futures, Expired Options, Expired Historical Candle) into a **single Markdown reference guide** so you can use it as a complete handbook?
