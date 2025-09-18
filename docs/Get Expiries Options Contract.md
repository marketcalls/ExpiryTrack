Here’s the **exact content** from your uploaded PDF in **Markdown format**:

---

# Expired Option Contracts | Upstox Developer API

## Developer API - Expired Instruments - Get Expired Option Contracts

### Get Expired Option Contracts

API to retrieve expired option contracts for an underlying instrument on a specified expiry date.
This API is useful for traders and analysts who want to analyze the historical performance of options and their underlying assets.

By providing the **instrument key** and **expiry date**, users can obtain detailed information about the expired options, including their trading symbols, strike prices, lot sizes, and other relevant attributes.

> **NOTE:**
>
> * Expired Option Contracts is currently not available for the **MCX**.
> * This API is specifically designed for expired option contracts. For current or active contracts, please refer to either **Instrument JSON** or the **Get Option Contracts API**.
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
| expiry\_date    | true     | string | Expiry date for which expired option contracts are required in format: `YYYY-MM-DD`.                             |

---

## Responses

* **200**

### Response Body

| Name   | Type   | Description                                                                                    |
| ------ | ------ | ---------------------------------------------------------------------------------------------- |
| status | string | A string indicating the outcome of the request. Typically `success` for successful operations. |
| data   | object | Data object for expired option contracts.                                                      |

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
      "expiry": "2025-04-17",
      "instrument_key": "NSE_FO|47983|17-04-2025",
      "exchange_token": "47983",
      "trading_symbol": "NIFTY 20400 PE 17 APR 25",
      "tick_size": 5,
      "lot_size": 75,
      "instrument_type": "PE",
      "freeze_quantity": 1800,
      "underlying_key": "NSE_INDEX|Nifty 50",
      "underlying_type": "INDEX",
      "underlying_symbol": "NIFTY",
      "strike_price": 20400,
      "minimum_lot": 75,
      "weekly": true
    },
    {
      "name": "NIFTY",
      "segment": "NSE_FO",
      "exchange": "NSE",
      "expiry": "2025-04-17",
      "instrument_key": "NSE_FO|47982|17-04-2025",
      "exchange_token": "47982",
      "trading_symbol": "NIFTY 20400 CE 17 APR 25",
      "tick_size": 5,
      "lot_size": 75,
      "instrument_type": "CE",
      "freeze_quantity": 1800,
      "underlying_key": "NSE_INDEX|Nifty 50",
      "underlying_type": "INDEX",
      "underlying_symbol": "NIFTY",
      "strike_price": 20400,
      "minimum_lot": 75,
      "weekly": true
    }
  ]
}
```

---

## Data Object Fields

| Field                      | Type    | Description                                                                                                                                                      |
| -------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| data\[].name               | string  | The name of the option.                                                                                                                                          |
| data\[].segment            | string  | The market segment of the option.<br>Possible values: `NSE_EQ`, `NSE_INDEX`, `NSE_FO`, `NCD_FO`, `BSE_EQ`, `BSE_INDEX`, `BSE_FO`, `BCD_FO`, `MCX_FO`, `NSE_COM`. |
| data\[].exchange           | string  | Exchange to which the instrument is associated. Possible values: `NSE`, `BSE`, `MCX`.                                                                            |
| data\[].expiry             | string  | Expiry date (for derivatives) in format `YYYY-MM-dd`.                                                                                                            |
| data\[].instrument\_key    | string  | Also referred to as `expired_instrument_key`. Combination of instrument\_key and expiry date.                                                                    |
| data\[].exchange\_token    | string  | The exchange-specific token for the option.                                                                                                                      |
| data\[].trading\_symbol    | string  | The symbol used for trading the option.<br>Format: `<underlying_symbol> <strike_price> <CE/PE> <expiry in dd MMM yy>`.                                           |
| data\[].tick\_size         | number  | The minimum price movement of the option.                                                                                                                        |
| data\[].lot\_size          | number  | The size of one lot of the option.                                                                                                                               |
| data\[].instrument\_type   | string  | The type of the option instrument. Possible values: `CE`, `PE`.                                                                                                  |
| data\[].freeze\_quantity   | number  | The maximum quantity that can be frozen.                                                                                                                         |
| data\[].underlying\_key    | string  | The instrument\_key for the underlying asset.                                                                                                                    |
| data\[].underlying\_type   | string  | The type of the underlying asset. Possible values: `COM`, `INDEX`, `EQUITY`, `CUR`, `IRD`.                                                                       |
| data\[].underlying\_symbol | string  | The symbol of the underlying asset.                                                                                                                              |
| data\[].strike\_price      | number  | The strike price for the option.                                                                                                                                 |
| data\[].minimum\_lot       | number  | The minimum lot size for the option.                                                                                                                             |
| data\[].weekly             | boolean | Indicates if the option is weekly.                                                                                                                               |

---

## Request

**Endpoint:**

```
GET  /expired-instruments/option/contract
```

**cURL Example:**

```bash
curl -L -X GET 'https://api.upstox.com/v2/expired-instruments/option/contract' \
-H 'Accept: application/json'
```

---

🔗 [More API Examples](https://upstox.com/developer/api-documentation/example-code/expired-instruments/get-expired-option-contracts)

---

Do you want me to combine **all three (Expiries, Expired Futures, Expired Options)** into a **single Markdown reference guide** for easier use?
