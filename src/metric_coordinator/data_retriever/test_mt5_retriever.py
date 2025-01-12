from metric_coordinator.data_retriever.mt5_data_retriever import MT5DataRetriever
from metric_coordinator.configs import settings

if __name__ == "__main__":
    retriever = MT5DataRetriever(server=settings.MT_SERVER, server_name=settings.SERVER_NAME, login=settings.MT_LOGIN, password=settings.MT_PASSWORD)
    data = retriever.retrieve_data(from_time=0, to_time=1731738718, filters={"group_by": "Login", "logins": [100001,500390]})
    print(retriever.last_retrieve_timestamp)
    print(data)
    assert data.shape[0] > 0
