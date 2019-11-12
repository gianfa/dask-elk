from elasticsearch import RequestsHttpConnection


SIMPLEPROXY = {'https': 'http://user:pw@proxy.org:port'}

class BasicProxyConnection(RequestsHttpConnection):
    '''Simple connection class that allows to commmunicate through proxy
    
    This fixes the *SerializationError: Unknown mimetype, unable to deserialize: text/html*,
    raised when the client reaches ELK, but is unable to effectively communicate with it.

    Example:
        from dask_elk.client import DaskElasticClient
        from dask_elk.proxy_utility import SIMPLEPROXY, BasicProxyConnection

        # First create a client
        client = DaskElasticClient( host='localhost', port= 9200,
            connection_class = BasicProxyConnection,  proxies = SIMPLEPROXY,
            translate_localhost = False
        )

        index = 'profile'
        df = client.read( index=index )
    
    References:
        * https://github.com/elastic/elasticsearch-py/issues/275#issuecomment-218403073
        * https://elasticsearch-py.readthedocs.io/en/master/transports.html#connection
    
    '''
    def __init__(self, *args, **kwargs):
        proxies = kwargs.pop('proxies', {})
        super(BasicProxyConnection, self).__init__(*args, **kwargs)
        self.session.proxies = proxies