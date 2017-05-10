try:
    import os, sys, json, argparse, urllib, urllib2, requests, certifi, threading
    from csv import reader
    from zipfile import ZipFile
    from json import dumps, loads, load, dump
    from itertools import islice, chain
    from elasticsearch import Elasticsearch, RequestsHttpConnection, serializer, compat, exceptions
    from elasticsearch.helpers import bulk
except Exception as e: print e

# 1st Attribution: http://stackoverflow.com/questions/38209061/django-elasticsearch-aws-httplib-unicodedecodeerror/38371830
# 2nd Attribution: https://docs.python.org/2/library/json.html#basic-usage
class JSONSerializerPython2(serializer.JSONSerializer):
    def dumps(self, data):
        if isinstance(data, compat.string_types): return data
        try: return dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e: raise exceptions.SerializationError(data, e)

# New endpoint
es = Elasticsearch(
    ['<>'],
    port=9243,
    http_auth='elastic' + ":" + '<>',
    serializer=JSONSerializerPython2(),
    ca_certs=certifi.where()
)

airports = {}
carriers = {}
planes = {}

def read_supporting_files(directory='C:/Users/mramd/Documents/CS-GY 9223 - Big Data Programming/Project/'):
    with open(directory + 'airports.csv', 'r') as r:
        next(r)
        for line in reader(r):
            try:
                airports[line[0]] = {
                    'iata': line[0],
                    'airport': line[1],
                    'city': line[2],
                    'state': line[3],
                    'country': line[4],
                    'lat': float(line[5]),
                    'long': float(line[6])
                }
            except Exception as e:
                print e
    with open(directory + 'carriers.csv', 'r') as r:
        next(r)
        for line in reader(r):
            try: carriers[line[0]] = {'Code': line[0], 'Description': line[1]}
            except Exception as e: print e
    # Plane Data is not used - planes dict is not used
    return True

read_supporting_files()
res_dir = 'C:/Users/mramd/Documents/CS-GY 9223 - Big Data Programming/Project/Results/'
def query1():  # Flights per month
    with open(res_dir + 'query1.csv', 'r') as r:
        next(r)
        query_mapping = {
            'mappings': {
                'queries': {
                    'properties': {
                        'count': {'type': 'integer'},
                        'month': {'type': 'integer'},
                        'destination': {'type': 'string'},
                        'origin': {'type': 'string'},
                        'destination_coordinates': {'type': 'geo_point'},
                        'origin_coordinates': {'type': 'geo_point'}
                    }
                }
            }
        }
        #es.indices.delete(index='query1', ignore=[400, 404])
        es.indices.create(index='query1', body=query_mapping, ignore=400)
        count = 1
        for line in reader(r):
            try:
                doc = {
                    'count': int(line[0]),
                    'month': int(line[1]),
                    'destination': line[3],
                    'origin': line[2],
                    'destination_coordinates': str(airports[line[3]]['lat']) + ', ' + str(airports[line[3]]['long']),
                    'origin_coordinates': str(airports[line[2]]['lat']) + ', ' + str(airports[line[2]]['long'])
                }
                yield {
                    "_index": 'query1',
                    "_type": 'query1',
                    "_source": doc
                }
                #es.index(index='query1', doc_type='query1', id=count, body=doc)
                count += 1
            except Exception as e:
                print e

def query3():  # Flights per weekday
    with open(res_dir + 'query3.csv', 'r') as r:
        next(r)
        query_mapping = {
            'mappings': {
                'queries': {
                    'properties': {
                        'count': {'type': 'integer'},
                        'day': {'type': 'integer'},
                        'destination': {'type': 'string'},
                        'origin': {'type': 'string'},
                        'destination_coordinates': {'type': 'geo_point'},
                        'origin_coordinates': {'type': 'geo_point'}
                    }
                }
            }
        }
        #es.indices.delete(index='query3', ignore=[400, 404])
        es.indices.create(index='query3', body=query_mapping, ignore=400)
        count = 1
        for line in reader(r):
            try:
                doc = {
                    'count': int(line[0]),
                    'day': int(line[1]),
                    'destination': line[3],
                    'origin': line[2],
                    'destination_coordinates': str(airports[line[3]]['lat']) + ', ' + str(airports[line[3]]['long']),
                    'origin_coordinates': str(airports[line[2]]['lat']) + ', ' + str(airports[line[2]]['long'])
                }
                yield {
                    "_index": 'query3',
                    "_type": 'query3',
                    "_source": doc
                }
                #es.index(index='query3', doc_type='query3', id=count, body=doc)
                count += 1
            except Exception as e:
                print e

def query4():
    query_mapping = {
        'mappings': {
            'queries': {
                'properties': {
                    'delayed': {'type': 'integer'},
                    'diverted': {'type': 'integer'},
                    'count': {'type': 'integer'}
                }
            }
        }
    }
    es.indices.delete(index='query4', ignore=[400, 404])
    es.indices.create(index='query4', body=query_mapping, ignore=400)
    es.index(index='query4', doc_type='query4', id=1, body={'delayed': 0, 'diverted': 1, 'count': 1565723})
    es.index(index='query4', doc_type='query4', id=2, body={'delayed': 1, 'diverted': 0, 'count': 152058})

def query5():  # Count of delays/cancellation per month
    with open(res_dir + 'query5.csv', 'r') as r:
        next(r)
        query_mapping = {
            'mappings': {
                'queries': {
                    'properties': {
                        'diverted': {'type': 'integer'},
                        'cancelled': {'type': 'integer'},
                        'month': {'type': 'integer'},
                        'count': {'type': 'integer'}
                    }
                }
            }
        }
        #es.indices.delete(index='query5', ignore=[400, 404])
        es.indices.create(index='query5', body=query_mapping, ignore=400)
        count = 1
        for line in reader(r):
            try:
                doc = {
                    'diverted': int(line[0]),
                    'cancelled': int(line[1]),
                    'month': int(line[2]),
                    'count': int(line[3])
                }
                yield {
                    "_index": 'query5',
                    "_type": 'query5',
                    "_source": doc
                }
                #es.index(index='query5', doc_type='query5', id=count, body=doc)
                count += 1
            except Exception as e:
                print e

def query7():  # Origin/Dest Diverted
    with open(res_dir + 'query7.csv', 'r') as r:
        next(r)
        query_mapping = {
            'mappings': {
                'queries': {
                    'properties': {
                        'origin': {'type': 'string'},
                        'destination': {'type': 'string'},
                        'count': {'type': 'integer'}
                    }
                }
            }
        }
        #es.indices.delete(index='query7', ignore=[400, 404])
        es.indices.create(index='query7', body=query_mapping, ignore=400)
        count = 1
        for line in reader(r):
            try:
                doc = {
                    'origin': line[0],
                    'destination': line[1],
                    'count': int(line[2])
                }
                yield {
                    "_index": 'query7',
                    "_type": 'query7',
                    "_source": doc
                }
                #es.index(index='query7', doc_type='query7', id=count, body=doc)
                count += 1
            except Exception as e:
                print e

def query8():  # Origin/Dest Cancelled
    with open(res_dir + 'query8.csv', 'r') as r:
        next(r)
        query_mapping = {
            'mappings': {
                'queries': {
                    'properties': {
                        'origin': {'type': 'string'},
                        'destination': {'type': 'string'},
                        'count': {'type': 'integer'}
                    }
                }
            }
        }
        #es.indices.delete(index='query8', ignore=[400, 404])
        es.indices.create(index='query8', body=query_mapping, ignore=400)
        count = 1
        for line in reader(r):
            try:
                doc = {
                    'origin': line[0],
                    'destination': line[1],
                    'count': int(line[2])
                }
                yield {
                    "_index": 'query8',
                    "_type": 'query8',
                    "_source": doc
                }
                #es.index(index='query8', doc_type='query8', id=count, body=doc)
                count += 1
            except Exception as e:
                print e

def query9_10_11_12():
    query_mapping = {
        'mappings': {
            'queries': {
                'properties': {
                    'season': {'type': 'string'},
                    'count': {'type': 'integer'}
                }
            }
        }
    }
    es.indices.delete(index='query9_10_11_12', ignore=[400, 404])
    es.indices.create(index='query9_10_11_12', body=query_mapping, ignore=400)
    es.index(index='query9_10_11_12', doc_type='query9_10_11_12', id=1, body={'season': 'winter', 'count': 17061516})
    es.index(index='query9_10_11_12', doc_type='query9_10_11_12', id=2, body={'season': 'spring', 'count': 17792588})
    es.index(index='query9_10_11_12', doc_type='query9_10_11_12', id=3, body={'season': 'summer', 'count': 18192786})
    es.index(index='query9_10_11_12', doc_type='query9_10_11_12', id=4, body={'season': 'autumn', 'count': 17151172})

def query13():  # Winter destinations
    with open(res_dir + 'query13.csv', 'r') as r:
        next(r)
        query_mapping = {
            'mappings': {
                'queries': {
                    'properties': {
                        'destination': {'type': 'string'},
                        'count': {'type': 'integer'}
                    }
                }
            }
        }
        #es.indices.delete(index='query13', ignore=[400, 404])
        es.indices.create(index='query13', body=query_mapping, ignore=400)
        count = 1
        for line in reader(r):
            try:
                doc = {
                    'destination': line[0],
                    'count': int(line[1])
                }
                yield {
                    "_index": 'query13',
                    "_type": 'query13',
                    "_source": doc
                }
                #es.index(index='query13', doc_type='query13', id=count, body=doc)
                count += 1
            except Exception as e:
                print e

def query14():  # Spring destinations
    with open(res_dir + 'query14.csv', 'r') as r:
        next(r)
        query_mapping = {
            'mappings': {
                'queries': {
                    'properties': {
                        'destination': {'type': 'string'},
                        'count': {'type': 'integer'}
                    }
                }
            }
        }
        #es.indices.delete(index='query14', ignore=[400, 404])
        es.indices.create(index='query14', body=query_mapping, ignore=400)
        count = 1
        for line in reader(r):
            try:
                doc = {
                    'destination': line[0],
                    'count': int(line[1])
                }
                yield {
                    "_index": 'query14',
                    "_type": 'query14',
                    "_source": doc
                }
                #es.index(index='query14', doc_type='query14', id=count, body=doc)
                count += 1
            except Exception as e:
                print e

def query15():  # Summer destinations
    with open(res_dir + 'query15.csv', 'r') as r:
        next(r)
        query_mapping = {
            'mappings': {
                'queries': {
                    'properties': {
                        'destination': {'type': 'string'},
                        'count': {'type': 'integer'}
                    }
                }
            }
        }
        #es.indices.delete(index='query15', ignore=[400, 404])
        es.indices.create(index='query15', body=query_mapping, ignore=400)
        count = 1
        for line in reader(r):
            try:
                doc = {
                    'destination': line[0],
                    'count': int(line[1])
                }
                yield {
                    "_index": 'query15',
                    "_type": 'query15',
                    "_source": doc
                }
                #es.index(index='query15', doc_type='query15', id=count, body=doc)
                count += 1
            except Exception as e:
                print e

def query16():  # Autumn destinations
    with open(res_dir + 'query16.csv', 'r') as r:
        next(r)
        query_mapping = {
            'mappings': {
                'queries': {
                    'properties': {
                        'destination': {'type': 'string'},
                        'count': {'type': 'integer'}
                    }
                }
            }
        }
        #es.indices.delete(index='query16', ignore=[400, 404])
        es.indices.create(index='query16', body=query_mapping, ignore=400)
        count = 1
        for line in reader(r):
            try:
                doc = {
                    'destination': line[0],
                    'count': int(line[1])
                }
                yield {
                    "_index": 'query16',
                    "_type": 'query16',
                    "_source": doc
                }
                #es.index(index='query16', doc_type='query16', id=count, body=doc)
                count += 1
            except Exception as e:
                print e

def query17():
    with open(res_dir + 'query17.csv', 'r') as r:
        next(r)
        query_mapping = {
            'mappings': {
                'queries': {
                    'properties': {
                        'average-arrival-delay': {'type': 'double'},
                        'flight-num': {'type': 'string'}
                    }
                }
            }
        }
        #es.indices.delete(index='query17', ignore=[400, 404])
        es.indices.create(index='query17', body=query_mapping, ignore=400)
        count = 1
        for line in reader(r):
            try:
                doc = {
                    'average-arrival-delay': float(line[0]),
                    'flight-num': line[1]
                }
                yield {
                    "_index": 'query17',
                    "_type": 'query17',
                    "_source": doc
                }
                #es.index(index='query17', doc_type='query17', id=count, body=doc)
                count += 1
            except Exception as e:
                print e

def query18():
    with open(res_dir + 'query18.csv', 'r') as r:
        next(r)
        query_mapping = {
            'mappings': {
                'queries': {
                    'properties': {
                        'average-departure-delay': {'type': 'double'},
                        'flight-num': {'type': 'string'}
                    }
                }
            }
        }
        #es.indices.delete(index='query18', ignore=[400, 404])
        es.indices.create(index='query18', body=query_mapping, ignore=400)
        count = 1
        for line in reader(r):
            try:
                doc = {
                    'average-departure-delay': float(line[0]),
                    'flight-num': line[1]
                }
                yield {
                    "_index": 'query18',
                    "_type": 'query18',
                    "_source": doc
                }
                #es.index(index='query18', doc_type='query18', id=count, body=doc)
                count += 1
            except Exception as e:
                print e




if __name__ == '__main__':
    
    '''
    es.indices.delete(index='query1', ignore=[400, 404])
    es.indices.delete(index='query3', ignore=[400, 404])
    es.indices.delete(index='query4', ignore=[400, 404])
    es.indices.delete(index='query5', ignore=[400, 404])
    es.indices.delete(index='query7', ignore=[400, 404])
    es.indices.delete(index='query8', ignore=[400, 404])
    es.indices.delete(index='query9_10_11_12', ignore=[400, 404])
    es.indices.delete(index='query13', ignore=[400, 404])
    es.indices.delete(index='query14', ignore=[400, 404])
    es.indices.delete(index='query15', ignore=[400, 404])
    es.indices.delete(index='query16', ignore=[400, 404])
    '''


    #success, _ = bulk(es, query18())
    #success, _ = bulk(es, query18())
    #success, _ = bulk(es, query1())
    #success, _ = bulk(es, query3())
    #query4()
    #success, _ = bulk(es, query5())
    #success, _ = bulk(es, query7())
    #success, _ = bulk(es, query8())
    #query9_10_11_12()
    #success, _ = bulk(es, query13())
    #success, _ = bulk(es, query14())
    #success, _ = bulk(es, query15())
    #success, _ = bulk(es, query16())


    print 'Done'
