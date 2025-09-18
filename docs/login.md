Here’s the content from your **Upstox Developer API Authentication** PDF formatted in **Markdown** for clarity:

---

# Upstox Developer API - Authentication

## Overview

Upstox uses **OAuth 2.0** for customer authentication and login.

* All logins are handled by **upstox.com**.
* No public endpoint exists for external applications to directly log users in.
* For security and compliance, **only Upstox handles logins and logouts**.

---

## Perform Authentication

The login window is hosted at:

```
https://api.upstox.com/v2/login/authorization/dialog
```

Your client app must open this URL in a **WebView** (or similar) and pass the following parameters:

| Parameter       | Description                              |
| --------------- | ---------------------------------------- |
| `client_id`     | API key from app generation.             |
| `redirect_uri`  | Redirect URL (must match app settings).  |
| `state`         | Optional. Returned after authentication. |
| `response_type` | Always `code`.                           |

### Sample URL

```
https://api.upstox.com/v2/login/authorization/dialog?
response_type=code&
client_id=<Your-API-Key-Here>&
redirect_uri=<Your-Redirect-URI-Here>&
state=<Your-Optional-State-Parameter-Here>
```

⚠️ **Note:**

* `client_id` = API Key (not UCC)
* `client_secret` = API Secret
* If you see **Invalid Credentials**, check `client_id`, `redirect_uri`, and `response_type`.
* Upstox supports **TOTP** for 2FA (recommended over SMS OTP).

---

## Receive Auth Code

On successful authentication, Upstox redirects to:

```
https://<redirect_uri>?code=<AUTH_CODE>&state=<STATE>
```

### Returned parameters:

* `code` → used to generate `access_token`.
* `state` → returned if passed initially.

---

## Generate Access Token

Use the `code` to make a **server-to-server call**:

**Endpoint:**

```
POST https://api.upstox.com/v2/login/authorization/token
```

**Parameters:**

| Parameter       | Description                     |
| --------------- | ------------------------------- |
| `code`          | Auth code from redirect.        |
| `client_id`     | API key from app generation.    |
| `client_secret` | API secret (keep confidential). |
| `redirect_uri`  | Same as app generation.         |
| `grant_type`    | Always `authorization_code`.    |

### Example cURL

```bash
curl -X 'POST' 'https://api.upstox.com/v2/login/authorization/token' \
-H 'accept: application/json' \
-H 'Content-Type: application/x-www-form-urlencoded' \
-d 'code=<Your-Auth-Code-Here>&client_id=<Your-API-Key-Here>&client_secret=<Your-API-Secret-Here>&redirect_uri=<Your-Redirect-URI-Here>&grant_type=authorization_code'
```

✅ Response → Returns an **Access Token** to access Upstox APIs.

---

## Extended Token

* **Validity:** 1 year (or until user revokes access).
* **Usage:** Long-term, read-only API calls.
* **Available APIs:**

  * Get Positions
  * Get Holdings
  * Get Order Details
  * Get Order History
  * Get Order Book

👉 Extended tokens are available only for **multi-client apps** upon request via support.

---

Do you want me to also create a **ready-to-use Markdown `.md` file** from this so you can directly add it to your docs or GitHub repo?
