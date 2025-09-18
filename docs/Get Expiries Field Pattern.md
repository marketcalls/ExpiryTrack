Here’s the **exact content** from your uploaded PDF in **Markdown format**:

---

# Field Pattern | Upstox Developer API

## Developer API Appendix - Field Pattern

This section outlines the specific regex patterns required for various field inputs, ensuring data consistency and validation.
Refer to these specifications to avoid common input errors and streamline data submission processes.

---

### order\_id

**Pattern:**

```
^[-a-zA-Z0-9]+
```

---

### symbol

**Pattern:**

```
^(?:NSE_EQ|NSE_FO|NCD_FO|BSE_EQ|BSE_FO|BCD_FO|MCX_FO|NSE_COM|NSE_INDEX|BSE_INDEX|MCX_INDEX)\|[\w ]+(,(?:NSE_EQ|NSE_FO|NCD_FO|BSE_EQ|BSE_FO|BCD_FO|MCX_FO|NSE_COM|NSE_INDEX|BSE_INDEX|MCX_INDEX)\|[\w ]+)*?$
```

---

### financial\_year

**Pattern:**

```
^(0|[1-9][0-9]*)$
```

---

### instrumentKey

**Pattern:**

```
^(?:^NSE_EQ|NSE_FO|NCD_FO|BSE_EQ|BSE_FO|BCD_FO|MCX_FO|NSE_COM|NSE_INDEX|BSE_INDEX|MCX_INDEX)\|[\w ]+(,(?:NSE_EQ|NSE_FO|NCD_FO|BSE_EQ|BSE_FO|BCD_FO|MCX_FO|NSE_COM|NSE_INDEX|BSE_INDEX|MCX_INDEX)\|[\w ]+)*?$
```

---

### instrument\_token

**Pattern:**

```
^(?:^NSE_EQ|NSE_FO|NCD_FO|BSE_EQ|BSE_FO|BCD_FO|MCX_FO|NSE_COM|NSE_INDEX|BSE_INDEX|MCX_INDEX)\|[\w ]+(,(?:NSE_EQ|NSE_FO|NCD_FO|BSE_EQ|BSE_FO|BCD_FO|MCX_FO|NSE_COM|NSE_INDEX|BSE_INDEX|MCX_INDEX)\|[\w ]+)*?$
```

---

### instrument\_key

**Pattern:**

```
^(?:^NSE_EQ|NSE_FO|NCD_FO|BSE_EQ|BSE_FO|BCD_FO|MCX_FO|NSE_COM|NSE_INDEX|BSE_INDEX|MCX_INDEX)\|[\w ]+(,(?:NSE_EQ|NSE_FO|NCD_FO|BSE_EQ|BSE_FO|BCD_FO|MCX_FO|NSE_COM|NSE_INDEX|BSE_INDEX|MCX_INDEX)\|[\w ]+)*?$
```

---

### exchange

**Pattern:**

```
^(\s*|(?:NSE|NFO|CDS|BSE|BFO|BCD|MCX|NSCOM)+)$
```

---

### expired\_instrument\_key

**Pattern:**

```
^(?:NSE_EQ|NSE_FO|NCD_FO|BSE_EQ|BSE_FO|BCD_FO|MCX_FO|NSE_INDEX|BSE_INDEX|MCX_INDEX|NSE_COM)\|[\w\d\-]+\|(0[1-9]|[12]\d|3[01])-(0[1-9]|1[012])-(\d{4})$
```

---

Would you like me to **merge this Field Pattern doc** along with the earlier ones (Expiries, Expired Futures, Expired Options, Expired Historical Candle) into a **single Markdown handbook** so you have a complete reference?
