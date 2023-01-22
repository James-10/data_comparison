"""Module to generate some test data"""
from faker import Faker
import pandas as pd
from utils.logging import get_logger

LOGGER = get_logger()

faker = Faker()

def generate_fake_client_data() -> pd.DataFrame:
    client = {}
    for i in range(0, 400000):
        client[i] = {}
        client[i]['client_id'] = faker.random_number()
        client[i]['name'] = faker.name()
        client[i]['address']= faker.address()
        client[i]['email']= str(faker.email())
        client[i]['phone']= str(faker.phone_number())

    df=pd.DataFrame.from_dict(client).transpose()

    LOGGER.info("Dataframe created from fake data")

    return df