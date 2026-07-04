"""Static variables for the data-quality checks. Variables only, no functions."""

VALID_DRIVER_STATUS = ["ATIVO", "FERIAS", "AFASTADO", "DESLIGADO"]
VALID_VEHICLE_STATUS = ["ativo", "em_manutencao", "inativo"]
VALID_VEHICLE_TYPES = [
    "VUC",
    "Caminhão Toco",
    "Caminhão Truck",
    "Carreta Simples",
    "Carreta LS",
    "Bitrem",
]
VALID_GEOFENCE_TYPES = [
    "centro_distribuicao",
    "pedagio",
    "posto_combustivel",
    "cliente",
]
VALID_TRIP_STATUS = ["em_transito", "concluida", "cancelada", "atrasada"]
# Physically plausible ceiling for truck telemetry, in km/h. Values above
# it are device sentinels/glitches (the source uses 999), not real speed.
MAX_SPEED_KMH = 150

# Broad Brazil geographic bounds used as a first-pass GPS sanity check.
BRAZIL_LATITUDE_MIN = -33.75
BRAZIL_LATITUDE_MAX = 5.27
BRAZIL_LONGITUDE_MIN = -73.99
BRAZIL_LONGITUDE_MAX = -34.79
