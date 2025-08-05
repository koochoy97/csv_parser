import pandas as pd
from io import StringIO

def parse_csv_data(csv_text: str):
    df = pd.read_csv(StringIO(csv_text))
    return df.to_dict(orient="records")
