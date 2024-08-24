# imports
from ibis_analytics.catalog import Catalog


# functions
def load_table(table, table_name):
    # instantiate catalog
    catalog = Catalog()

    # load
    catalog.write_table(table, table_name)
