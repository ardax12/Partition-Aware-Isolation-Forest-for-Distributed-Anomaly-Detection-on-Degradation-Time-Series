SETTINGS = [
    "setting1",
    "setting2",
    "setting3",
]

SENSORS = [
    f"sensor{i}"
    for i in range(1, 22)
]

COLUMN_NAMES = [
    "engine_id",
    "cycle"
] +SETTINGS + SENSORS

TARGET_COLUMN = "RUL"

FEATURE_COLUMNS = SETTINGS + SENSORS + ["cycle"]

