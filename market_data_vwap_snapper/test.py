from datetime import datetime

changes = [
    ["buy", "0.0358", "90947.83"],
    ["buy", "0.0357", "124273.30"],
    ["sell", "0.0392", "9884.61"],
    ["sell", "0.0366", "11394.35"],
    ["sell", "0.0360", "1000.00"],
    ["sell", "0.0361", "31169.67"],
    ["sell", "0.0397", "1334.96"],
    ["sell", "0.0369", "15248.97"],
]

print([i for i in changes if i[0] == "sell"])
print([i for i in changes if i[0] == "buy"])
