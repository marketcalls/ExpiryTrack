Here’s the **exact content** from your uploaded PDF in **Markdown format**:

---

# Expired Future Contracts | Upstox Developer API

## Developer API - Expired Instruments - Get Expired Future Contracts

### Get Expired Future Contracts

API to retrieve expired future contracts for an underlying instrument on a specified expiry date.
This API is useful for traders and analysts who want to analyze the historical performance of futures and their underlying assets.

By providing the **instrument key** and **expiry date**, users can obtain detailed information about the expired futures, including their trading symbols, lot sizes, and other relevant attributes.

> **NOTE:**
>
> * Expired Future Contracts is currently not available for the **MCX**.
> * This API is specifically designed for expired future contracts. For current or active instruments, please refer to the Instrument JSON.
> * Before using this API, ensure to check the **Get Expiries API** to obtain the available expiry dates for the underlying instrument.

---

## Header Parameters

| Name          | Required | Type   | Description                                                                                    |
| ------------- | -------- | ------ | ---------------------------------------------------------------------------------------------- |
| Authorization | true     | string | Requires the format `Bearer access_token` where `access_token` is obtained from the Token API. |
| Accept        | true     | string | Defines the content format the client expects, should be set to `application/json`.            |

---

## Query Parameters

| Name            | Required | Type   | Description                                                                                                      |
| --------------- | -------- | ------ | ---------------------------------------------------------------------------------------------------------------- |
| instrument\_key | true     | string | Key of an underlying instrument. For the regex pattern applicable to this field, see the Field Pattern Appendix. |
| expiry\_date    | true     | string | Expiry date for which expired future contracts are required in format: `YYYY-MM-DD`.                             |

---

## Responses

* **200**

### Response Body

| Name   | Type   | Description                                                                                    |
| ------ | ------ | ---------------------------------------------------------------------------------------------- |
| status | string | A string indicating the outcome of the request. Typically `success` for successful operations. |
| data   | object | Data object for expired future contracts.                                                      |

---

### Example Response

```json
{
  "status": "success",
  "data": [
    {
      "name": "NIFTY",
      "segment": "NSE_FO",
      "exchange": "NSE",
      "expiry": "2025-04-24",
      "instrument_key": "NSE_FO|54452|24-04-2025",
      "exchange_token": "54452",
      "trading_symbol": "NIFTY FUT 24 APR 25",
      "tick_size": 10,
      "lot_size": 75,
      "instrument_type": "FUT",
      "freeze_quantity": 1800,
      "underlying_key": "NSE_INDEX|Nifty 50",
      "underlying_type": "INDEX",
      "underlying_symbol": "NIFTY",
      "minimum_lot": 75
    }
  ]
}
```

---

## Data Object Fields

| Field                      | Type   | Description                                                                                                                                                      |
| -------------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| data\[].name               | string | The name of the future.                                                                                                                                          |
| data\[].segment            | string | The market segment of the future.<br>Possible values: `NSE_EQ`, `NSE_INDEX`, `NSE_FO`, `NCD_FO`, `BSE_EQ`, `BSE_INDEX`, `BSE_FO`, `BCD_FO`, `MCX_FO`, `NSE_COM`. |
| data\[].exchange           | string | Exchange to which the instrument is associated. Possible values: `NSE`, `BSE`, `MCX`.                                                                            |
| data\[].expiry             | string | Expiry date (for derivatives) in format `YYYY-MM-dd`.                                                                                                            |
| data\[].instrument\_key    | string | Also referred to as `expired_instrument_key` for expired future contract. Combination of instrument\_key and expiry date.                                        |
| data\[].exchange\_token    | string | The exchange-specific token for the future.                                                                                                                      |
| data\[].trading\_symbol    | string | The symbol used for trading the future. Format: `<underlying_symbol> <expiry in dd MMM yy> FUT`.                                                                 |
| data\[].tick\_size         | number | The minimum price movement of the future.                                                                                                                        |
| data\[].lot\_size          | number | The size of one lot of the future.                                                                                                                               |
| data\[].instrument\_type   | string | The type of the instrument. For futures, this is always `FUT`.                                                                                                   |
| data\[].freeze\_quantity   | number | The maximum quantity that can be frozen.                                                                                                                         |
| data\[].underlying\_key    | string | The instrument\_key for the underlying asset.                                                                                                                    |
| data\[].underlying\_type   | string | The type of the underlying asset. Possible values: `COM`, `INDEX`, `EQUITY`, `CUR`, `IRD`.                                                                       |
| data\[].underlying\_symbol | string | The symbol of the underlying asset.                                                                                                                              |
| data\[].minimum\_lot       | number | The minimum lot size for the future.                                                                                                                             |

---

## Request

**Endpoint:**

```
GET  /expired-instruments/future/contract
```

**cURL Example:**

```bash
curl -L -X GET 'https://api.upstox.com/v2/expired-instruments/future/contract' \
-H 'Accept: application/json'
```

---

🔗 [More API Examples](https://upstox.com/developer/api-documentation/example-code/expired-instruments/get-expired-future-contracts)

---

Do you want me to also prepare this in **Bruno collection format** (like your earlier request for historical candles), so you can import and test it directly?
