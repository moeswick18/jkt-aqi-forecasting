import numpy as np


def concentration_to_aqi(aqi_parameter, Xx):
    """
    Parameters
    ----------
    aqi_parameter = pollutant name you want to calculate
    Xx = pollutant concentration

    Returns
    -------
    I = pollutant AQI value
    """
    aqi_conversion_table = {
        'pm25': {
            15.5: range(1, 50+1, 1),
            55.4: range(51, 100+1, 1),
            150.4: range(101, 200+1, 1),
            250.4: range(201, 300+1, 1),
            500: range(301, 301+1, 1)
        },
        'pm10': {
            50: range(1, 50+1, 1),
            150: range(51, 100+1, 1),
            350: range(101, 200+1, 1),
            420: range(201, 300+1, 1),
            500: range(301, 301+1, 1)
        },
        'so2': {
            52: range(1, 50+1, 1),
            180: range(51, 100+1, 1),
            400: range(101, 200+1, 1),
            800: range(201, 300+1, 1),
            1200: range(301, 301+1, 1)
        },
        'co': {
            4000: range(1, 50+1, 1),
            8000: range(51, 100+1, 1),
            15000: range(101, 200+1, 1),
            30000: range(201, 300+1, 1),
            45000: range(301, 301+1, 1)
        },
        'o3': {
            120: range(1, 50+1, 1),
            235: range(51, 100+1, 1),
            400: range(101, 200+1, 1),
            800: range(201, 300+1, 1),
            1000: range(301, 301+1, 1)
        },
        'no2': {
            80: range(1, 50+1, 1),
            200: range(51, 100+1, 1),
            1130: range(101, 200+1, 1),
            2260: range(201, 300+1, 1),
            3000: range(301, 301+1, 1)
        },
    }

    I = 0
    Ia = 0
    Ib = 0
    Xa = 0
    Xb = 0

    temp = list(aqi_conversion_table[aqi_parameter].keys())

    if Xx != 0 and ~np.isnan(Xx):
        for key in aqi_conversion_table[aqi_parameter].keys():
            if Xx <= key:
                Xa = key
                Xb = [temp[0] if Xx > temp[0] else 0][0]
                Ia = [max(aqi_conversion_table[aqi_parameter][key])][0]
                Ib = [max(aqi_conversion_table[aqi_parameter][temp[0]]) if Xx > temp[0] else min(aqi_conversion_table[aqi_parameter][temp[0]])][0]
                break
            elif Xx > temp[-1]:
                Xa = temp[-1]
                Xb = [temp[0] if Xx > temp[0] else 0][0]
                Ia = [max(aqi_conversion_table[aqi_parameter][temp[-1]])][0]
                Ib = [max(aqi_conversion_table[aqi_parameter][temp[0]]) if Xx > temp[0] else min(aqi_conversion_table[aqi_parameter][temp[0]])][0]
                break
        I = round(((Ia - Ib) / (Xa - Xb)) * (Xx - Xb) + Ib)
    elif Xx == 0 or np.isnan(Xx):
        I = Xx

    return I


def aqi_to_concentration(aqi_parameter, I):
    """
    Parameters
    ----------
    aqi_parameter = polutant name you want to calculate
    I = pollutant AQI value

    Returns
    -------
    Xx = pollutant concentration
    """
    aqi_conversion_table = {
        'pm25': {
            range(1, 50+1, 1): 15.5,
            range(51, 100+1, 1): 55.4,
            range(101, 200+1, 1): 150.4,
            range(201, 300+1, 1): 250.4,
            range(301, 301+1, 1): 500
        },
        'pm10': {
            range(1, 50+1, 1): 50,
            range(51, 100+1, 1): 150,
            range(101, 200+1, 1): 350,
            range(201, 300+1, 1): 420,
            range(301, 301+1, 1): 500
        },
        'so2': {
            range(1, 50+1, 1): 52,
            range(51, 100+1, 1): 180,
            range(101, 200+1, 1): 400,
            range(201, 300+1, 1): 800,
            range(301, 301+1, 1): 1200
        },
        'co': {
            range(1, 50+1, 1): 4000,
            range(51, 100+1, 1): 8000,
            range(101, 200+1, 1): 15000,
            range(201, 300+1, 1): 30000,
            range(301, 301+1, 1): 45000
        },
        'o3': {
            range(1, 50+1, 1): 120,
            range(51, 100+1, 1): 235,
            range(101, 200+1, 1): 400,
            range(201, 300+1, 1): 800,
            range(301, 301+1, 1): 1000
        },
        'no2': {
            range(1, 50+1, 1): 80,
            range(51, 100+1, 1): 200,
            range(101, 200+1, 1): 1130,
            range(201, 300+1, 1): 2260,
            range(301, 301+1, 1): 3000
        },
    }

    Xx = 0
    Xa = 0
    Xb = 0
    Ia = 0
    Ib = 0

    temp = list(aqi_conversion_table[aqi_parameter].keys())

    if I != 0 and ~np.isnan(I):
        for key in aqi_conversion_table[aqi_parameter].keys():
            if I in key:
                Xa = aqi_conversion_table[aqi_parameter][key]
                Xb = [aqi_conversion_table[aqi_parameter][temp[0]] if I > max(temp[0]) else 0][0]
                Ia = max(key)
                Ib = [max(temp[0]) if I > max(temp[0]) else 0][0]
                break
            elif I > max(temp[-1]):
                Xa = aqi_conversion_table[aqi_parameter][temp[-1]]
                Xb = aqi_conversion_table[aqi_parameter][temp[0]]
                Ia = max(temp[-1])
                Ib = [max(temp[0]) if I > max(temp[0]) else 0][0]
                break
        Xx = round(((Xa - Xb) / (Ia - Ib)) * (I - Ib) + Xb, 1)
    elif I == 0 or np.isnan(I):
        Xx = I

    return Xx
