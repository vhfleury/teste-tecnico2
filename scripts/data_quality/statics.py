"""Static variables for the data-quality checks. Variables only, no functions."""

VALID_DRIVER_STATUS = ["ATIVO", "FERIAS", "AFASTADO", "DESLIGADO"]
VALID_VEHICLE_STATUS = ["ATIVO", "EM_MANUTENCAO", "INATIVO"]
# Vehicle type keeps its mixed case: the config trims it but does not
# normalize (uppercase) it, so the reference values match the source.
VALID_VEHICLE_TYPES = [
    "VUC",
    "Caminhão Toco",
    "Caminhão Truck",
    "Carreta Simples",
    "Carreta LS",
    "Bitrem",
]
VALID_GEOFENCE_TYPES = [
    "CENTRO_DISTRIBUICAO",
    "PEDAGIO",
    "POSTO_COMBUSTIVEL",
    "CLIENTE",
]
VALID_TRIP_STATUS = ["EM_TRANSITO", "CONCLUIDA", "CANCELADA", "ATRASADA"]
# Physically plausible ceiling for truck telemetry, in km/h. Values above
# it are device sentinels/glitches (the source uses 999), not real speed.
MAX_SPEED_KMH = 150

# Broad Brazil geographic bounds used as a first-pass GPS sanity check.
BRAZIL_LATITUDE_MIN = -33.75
BRAZIL_LATITUDE_MAX = 5.27
BRAZIL_LONGITUDE_MIN = -73.99
BRAZIL_LONGITUDE_MAX = -34.79
