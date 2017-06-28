import csv
from pony.orm import *
from datetime import date
import requests
import os

db = Database()
# db.bind('sqlite', ':memory:')
db.bind('sqlite', 'cost_database.sqlite', create_db=True)

class PricePoint(db.Entity):
    """ORM for PricePoint."""
    id = PrimaryKey(int, auto=True)
    service_type = Required(str)
    price_per_hour = Required(float)
    resource_type = Required(str)
    effective_date = Required(date)
    location = Required(str)
    distro = Required(str)

db.generate_mapping(create_tables=True)

def convert_date(d):
    """Convert date from csv format to Python Date"""
    _d = d.split('-')
    return date(int(_d[0]), int(_d[1]), int(_d[2]))

@db_session
def create_pp(row):
    """Create a PricePoint from a row of the offer csv. Ignore duplicates"""
    # q = select(p for p in Product)
    # q2 = q.filter(price=100, name="iPod")

    duplicates = (select(p for p in PricePoint if p.service_type == 'EC2'
                                      and p.resource_type == row[18]
                                      and p.effective_date == convert_date(row[5])
                                      and p.location == row[16]
                                      and p.distro == row[37]
                                      ).count())
    if duplicates == 0:
        pp = PricePoint(service_type='EC2', price_per_hour=row[9], resource_type=row[18], effective_date=row[5], location=row[16], distro=row[37])
        commit()
    return


@db_session
def get_pps():
    return(list(select(p for p in PricePoint if p.price_per_hour > 10).order_by(PricePoint.price_per_hour)[:]))



def get_latest_cost_csv():
    """Download and parse the offer index document, then download the latest
    cost info in csv format. Return the path of the local csv file"""
    # index_req = requests.get('https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json')
    # index_json = index_req.json()
    # price_info_endpoint = index_json.get('offers').get('AmazonEC2').get('versionIndexUrl')
    # price_info_url = 'https://pricing.us-east-1.amazonaws.com' + price_info_endpoint
    # print(price_info_url)
    cost_csv_path = './cost2.csv'
    cost_info_url = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.csv'
    os.system('wget {url} -O {path}'.format(url=cost_info_url, path=cost_csv_path))
    return cost_csv_path

def read_cost_csv(csv_path):
    with open(csv_path, 'r') as csvfile:
        costreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in costreader:
            # Strip out first few lines of metadata
            if len(row) > 4:
                yield row

_filt = {3:'OnDemand', 37:'RHEL', 35:'Shared'}

def filter_csv_rows(_filter, rows):
    for row in rows:
        if all([row[x]==y for x,y in _filter.items()]):
            yield row

def consume_cost_csv(csv_path):
    """Open the csv file, extract only the relevant entries. If entry is not a
    duplicate, add it to the database as a PricePoint"""
    for row in filter_csv_rows(_filt, read_cost_csv(csv_path)):
        create_pp(row)

# cost_csv_path = get_latest_cost_csv()
consume_cost_csv('./cost.csv')
print(get_pps()[~0].price_per_hour)
print(len(get_pps()))
