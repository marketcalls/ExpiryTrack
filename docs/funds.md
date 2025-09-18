Here’s the **Get Fund and Margin - Upstox Developer API** section from your PDF converted into **Markdown format**:

---

# Upstox Developer API - Get Fund and Margin

## Overview

API to retrieve user funds data for **equity** and **commodity** markets.
It provides details such as:

* Margin utilized
* Available margin for trading
* Total pay-in amount during the day

⚠️ **Note:**
From **19th July 2025**, combined funds for both Equity and Commodity segments are returned in the `equity` object. See [Fund and Margin API Response Change announcement](https://upstox.com/developer/api-documentation/announcements/fund-margin-api-change) .

---

## Endpoint

```
GET /user/get-funds-and-margin
```

**Live URL:**

```
https://api.upstox.com/v2/user/get-funds-and-margin
```

---

## Header Parameters

| Name            | Required | Type   | Description                                     |
| --------------- | -------- | ------ | ----------------------------------------------- |
| `Authorization` | true     | string | `Bearer access_token` (obtained from Token API) |
| `Accept`        | true     | string | Must be `application/json`                      |

---

## Query Parameters

| Name      | Required | Type   | Description                                                                                                               |
| --------- | -------- | ------ | ------------------------------------------------------------------------------------------------------------------------- |
| `segment` | false    | string | Market segment. If not specified, response includes both equity & commodity.<br>Values: `SEC` (Equity), `COM` (Commodity) |

---

## Response

### Success (200)

```json
{
  "status": "success",
  "data": {
    "equity": {
      "used_margin": 0.8,
      "payin_amount": 200.0,
      "span_margin": 0.0,
      "adhoc_margin": 0.0,
      "notional_cash": 0.0,
      "available_margin": 15507.46,
      "exposure_margin": 0.0
    },
    "commodity": {
      "used_margin": 0,
      "payin_amount": 0,
      "span_margin": 0,
      "adhoc_margin": 0,
      "notional_cash": 0,
      "available_margin": 0,
      "exposure_margin": 0
    }
  }
}
```

### Fields

| Field              | Type   | Description                                                   |
| ------------------ | ------ | ------------------------------------------------------------- |
| `status`           | string | Outcome of request (`success` if successful).                 |
| `used_margin`      | float  | Amount blocked in open orders/positions. Negative = released. |
| `payin_amount`     | float  | Instant pay-in reflected here.                                |
| `span_margin`      | float  | Margin blocked on F\&O (SPAN).                                |
| `adhoc_margin`     | float  | Margin credited manually.                                     |
| `notional_cash`    | float  | Amount maintained for withdrawal.                             |
| `available_margin` | float  | Total margin available for trading.                           |
| `exposure_margin`  | float  | Margin blocked on F\&O (Exposure).                            |

---

## Example Request

```bash
curl -L -X GET 'https://api.upstox.com/v2/user/get-funds-and-margin' \
-H 'Accept: application/json' \
-H 'Authorization: Bearer <Your-Access-Token>'
```

---

## Service Availability

⚠️ The **Funds service is unavailable daily from 12:00 AM to 5:30 AM IST** due to maintenance.
Plan API usage accordingly.

---

Do you want me to now **combine this with the Authentication section** I converted earlier into a single Markdown `.md` file for your GitHub/docs?
