from modules.environ import (
    SF_INSTANCE_URL,
    SF_PASSWORD,
    SF_SECURITY_TOKEN,
    SF_USERNAME,
)
from modules.utils import tqdm_bar
from tqdm import tqdm
import simple_salesforce


class SalesforceConnection:
    def __init__(self, username, password, security_token, instance_url=None):
        self.username = username
        self.password = password
        self.security_token = security_token
        self.instance_url = instance_url

    def connect(self):
        return simple_salesforce.Salesforce(
            username=self.username,
            password=self.password,
            security_token=self.security_token,
            instance_url=self.instance_url,
        )

    def get_records(self, query):
        sf = self.connect()
        return sf.query_all(query)


def get_salesforce_records(query, ids, batch_size=300):
    """
    This function retrieves Salesforce records based on a query and a list of IDs, with an optional
    batch size parameter.

    :param query: The SOQL query to fetch records from Salesforce
    :param ids: A list of Salesforce record IDs to retrieve data for
    :param batch_size: The number of records to fetch in each batch. Default value is 300, defaults to
    300. Salesforce only allows a maximum of 300 records per query.
    :return: a list of Salesforce records that match the given query and IDs, with additional processing
    of the records to extract specific fields.
    """
    username = SF_USERNAME
    password = SF_PASSWORD
    security_token = SF_SECURITY_TOKEN
    instance_url = SF_INSTANCE_URL

    sf = SalesforceConnection(
        username=username,
        password=password,
        security_token=security_token,
        instance_url=instance_url,
    )
    salesforce_records = []

    ids = sorted(list(ids))

    total_batches = (len(ids) + batch_size - 1) // batch_size

    for i in tqdm(range(total_batches), desc="Fetching Salesforce Records"):
        id_batch = ids[i * batch_size : (i + 1) * batch_size]
        ids_string = ", ".join(f"'{id_}'" for id_ in id_batch)
        batch_query = query.format(ids_string)
        batch_results = sf.get_records(batch_query)
        salesforce_records.extend([dict(record) for record in batch_results["records"]])

        processed_records = []
        for record in batch_results["records"]:
            processed_record = {
                key: record[key] for key in ("Name", "ID_Cliente__c", "Operado_Por__c")
            }
            processed_records.append(processed_record)

        salesforce_records.extend(processed_records)

        # tqdm_bar(
        #     total=1,
        #     desc="Batch progress",
        #     bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt}",
        # )

    return salesforce_records


# Despues le dare uso.
# if __name__ == "__main__":

#     from environ import SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN, SF_INSTANCE_URL

#     print("Usuario de SF:", SF_USERNAME)
#     print("Instancia de SF:", SF_INSTANCE_URL)

#     sf = SalesforceConnection(
#         username=SF_USERNAME,
#         password=SF_PASSWORD,
#         security_token=SF_SECURITY_TOKEN,
#         instance_url=SF_INSTANCE_URL,
#     )
