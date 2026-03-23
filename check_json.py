import json

def check_expiries():
    print("Loading nse_master.json...")
    with open("nse_master.json", "r") as f:
        data = json.load(f)
        
    print(f"Loaded {len(data)} instruments.")
    
    # Check for anything expiring on March 17, 2026
    found = {}
    
    for inst in data:
        expiry = inst.get("expiry", "")
        if "2026-03-17" in str(expiry):
            # Collect unique names/symbols
            name = inst.get("name", "")
            if name not in found:
                found[name] = 0
            found[name] += 1
            
        elif "17-MAR-2026" in str(expiry).upper() or "17-MAR-26" in str(expiry).upper():
             name = inst.get("name", "")
             if name not in found:
                 found[name] = 0
             found[name] += 1
             
    print("\nInstruments with March 17, 2026 expiry:")
    if not found:
        print("None found!")
    else:
        for name, count in found.items():
            print(f"- {name}: {count} contracts")

    # What about NIFTY 50 expiries in March 2026?
    print("\nNIFTY 50 expiries in March 2026:")
    nifty_exp = set()
    for inst in data:
        if inst.get("name") == "NIFTY" and str(inst.get("expiry", "")).startswith("2026-03"):
            nifty_exp.add(inst.get("expiry"))
    
    for exp in sorted(list(nifty_exp)):
        print(exp)

if __name__ == "__main__":
    check_expiries()
