from pathlib import Path
from faker import Faker
import random
from datetime import datetime, timedelta, timezone
import polars as pd

fake = Faker()


def make_sample_events_faker(n=20, duplicate_fraction=0.2, seed=42) -> pd.DataFrame:
    random.seed(seed)
    Faker.seed(seed)

    # Focus around "same time yesterday"
    base_time = datetime.now(timezone.utc) - timedelta(days=1)

    # -------------------------
    # 1. event_id with duplicates
    # -------------------------
    base_ids = [fake.uuid4() for _ in range(n)]
    dup_count = int(n * duplicate_fraction)
    dup_ids = random.sample(base_ids, dup_count)

    event_ids = []
    for eid in base_ids:
        event_ids.append(eid)
        if eid in dup_ids:
            extra = random.randint(1, 3)
            event_ids.extend([eid] * extra)

    random.shuffle(event_ids)

    # -------------------------
    # 2. client_tstamp generation
    # -------------------------
    ts_for_event = {}
    client_ts = []

    def make_base_ts():
        r = random.random()

        if r < 0.55:
            return base_time - timedelta(seconds=random.randint(1, 120))
        elif r < 0.75:
            return base_time - timedelta(hours=random.uniform(1, 48))
        elif r < 0.90:
            return base_time - timedelta(days=random.uniform(2, 14))
        else:
            return base_time + timedelta(minutes=random.randint(1, 45))

    for eid in event_ids:
        if eid not in ts_for_event:
            ts_for_event[eid] = make_base_ts()
        client_ts.append(ts_for_event[eid])

    # -------------------------
    # 3. collector_tstamp (mostly near base_time, late arrivals possible)
    # -------------------------
    collector_ts = []

    for ct in client_ts:
        r = random.random()

        # Most common: 1–30 seconds around the base_time
        if r < 0.75:
            col = base_time + timedelta(seconds=random.randint(1, 30))

        # Late arrival: minutes to days after client_tstamp
        elif r < 0.95:
            col = ct + timedelta(
                seconds=random.randint(30, 120)
                + random.randint(0, 3600)  # up to 1 hour
                + random.randint(0, 86400 * 2)  # up to 2 days
            )

        # Rare case: collector slightly before client (clock skew)
        else:
            col = ct - timedelta(seconds=random.randint(1, 20))

        collector_ts.append(col)

    # Make collector unique by adding micro-nudge
    collector_ts = [c + timedelta(microseconds=i) for i, c in enumerate(collector_ts)]

    # -------------------------
    # 4. derived_tstamp follows client_tstamp tightly
    # -------------------------
    derived_ts = [
        ct + timedelta(microseconds=random.randint(-800, 800)) for ct in client_ts
    ]

    # -------------------------
    # 5. load_tstamp (close to collector)
    # -------------------------
    load_ts = [
        col + timedelta(milliseconds=random.randint(0, 30)) for col in collector_ts
    ]

    df = pd.DataFrame(
        {
            "event_id": event_ids,
            "client_tstamp": client_ts,
            "derived_tstamp": derived_ts,
            "collector_tstamp": collector_ts,
            "load_tstamp": load_ts,
        }
    )

    return df.sort(["event_id", "collector_tstamp"])


if __name__ == "__main__":
    df = make_sample_events_faker(n=50)
    df.write_csv(Path(__file__).parent.parent / "seeds" / "sample_events.csv")
