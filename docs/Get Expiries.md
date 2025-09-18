Here’s the **exact content** from your uploaded PDF in **Markdown format**:

---

# Get Expiries | Upstox Developer API

## Developer API Expired Instruments Get Expiries

### Get Expiries

API to retrieve all the expiries for a given underlying instrument. This API is useful for traders and analysts who need to know all available expiry dates for a specific instrument to plan their trading strategies accordingly.

> **NOTE:**
> Expiries is currently not available for the **MCX**.
> The API will retrieve the complete list of expiry dates (including weekly and monthly) for the instrument, covering up to six months of historical expiries.

---

## Header Parameters

| Name          | Required | Type   | Description                                                                                    |
| ------------- | -------- | ------ | ---------------------------------------------------------------------------------------------- |
| Authorization | true     | string | Requires the format `Bearer access_token` where `access_token` is obtained from the Token API. |
| Accept        | true     | string | Defines the content format the client expects, should be set to `application/json`.            |

---

## Path Parameters

| Name            | Required | Type   | Description                                                                                                  |
| --------------- | -------- | ------ | ------------------------------------------------------------------------------------------------------------ |
| instrument\_key | true     | string | Key of an underlying symbol. For the regex pattern applicable to this field, see the Field Pattern Appendix. |

---

## Responses

* **200**

### Response Body

| Name   | Type   | Description                                                                                    |
| ------ | ------ | ---------------------------------------------------------------------------------------------- |
| status | string | A string indicating the outcome of the request. Typically `success` for successful operations. |
| data   | array  | List of all available expiry dates in format `YYYY-MM-dd`.                                     |

---

### Example Response

```json
{
  "status": "success",
  "data": [
    "2024-10-03",
    "2024-10-10",
    "2024-10-17",
    "2024-10-24",
    "2024-10-31",
    "2024-11-07",
    "2024-11-14",
    "2024-11-21",
    "2024-11-28",
    "2024-12-05",
    "2024-12-12",
    "2024-12-19",
    "2024-12-26",
    "2025-01-02",
    "2025-01-09",
    "2025-01-16",
    "2025-01-23",
    "2025-01-30",
    "2025-02-06",
    "2025-02-13",
    "2025-02-20",
    "2025-02-27",
    "2025-03-06",
    "2025-03-13",
    "2025-03-20",
    "2025-03-27",
    "2025-04-03",
    "2025-04-09",
    "2025-04-17"
  ]
}
```

---

## Request

**Endpoint:**

```
GET  /expired-instruments/expiries
```

**cURL Example:**

```bash
curl -L -X GET 'https://api.upstox.com/v2/expired-instruments/expiries' \
-H 'Accept: application/json'
```

---

🔗 [More API Examples](https://upstox.com/developer/api-documentation/example-code/expired-instruments/get-expiries)

---

Do you want me to also convert this into a **Bruno collection format** (like I did earlier for historical candles), so you can directly import and test it?
