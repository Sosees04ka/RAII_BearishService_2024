import numpy as np
from repository import HouseRepository

class Matrix:
    @classmethod
    def solve_tariffs(cls, data, max_systems=5):
        data_by_volume_count = {3: [], 2: [], 1: []}

        for row in data:
            volumes = []
            volume_labels = []

            if row.volume_cold is not None:
                volumes.append(row.volume_cold)
                volume_labels.append('cold_water_tariff')
            if row.volume_hot is not None:
                volumes.append(row.volume_hot)
                volume_labels.append('hot_water_tariff')
            if row.volume_electr is not None:
                volumes.append(row.volume_electr)
                volume_labels.append('electricity_tariff')

            if len(volumes) > 0:
                data_by_volume_count[len(volumes)].append((volumes, row.income, volume_labels))

        for volume_count in range(3, 0, -1):
            if data_by_volume_count[volume_count]:
                data_to_use = data_by_volume_count[volume_count]
                break
        else:
            return None

        def solve_system(data_subset):
            A = np.array([d[0] for d in data_subset])
            b = np.array([d[1] for d in data_subset])
            labels = data_subset[0][2]
            tariffs, _, _, _ = np.linalg.lstsq(A, b, rcond=None)
            return dict(zip(labels, tariffs))

        results = []
        count = 0
        for i in range(len(data_to_use)):
            for j in range(i + 1, len(data_to_use) + 1):
                subset = data_to_use[i:j]
                if len(subset) >= 2:
                    results.append(solve_system(subset))
                    count += 1
                    if count >= max_systems:
                        break
            if count >= max_systems:
                break

        avg_tariffs = {}
        count_tariffs = {}

        for result in results:
            for key, value in result.items():
                if key not in avg_tariffs:
                    avg_tariffs[key] = 0
                    count_tariffs[key] = 0
                avg_tariffs[key] += value
                count_tariffs[key] += 1

        avg_tariffs = {key: avg_tariffs[key] / count_tariffs[key] for key in avg_tariffs}

        all_tariffs = ['cold_water_tariff', 'hot_water_tariff', 'electricity_tariff']
        for tariff in all_tariffs:
            if tariff not in avg_tariffs:
                avg_tariffs[tariff] = None

        return avg_tariffs

    @classmethod
    async def get_tariffs(cls, house: int):
        data = await HouseRepository.get_house_data(house)

        if not data:
            return None

        tariffs = cls.solve_tariffs(data)
        if tariffs is None:
            return None

        return tariffs
