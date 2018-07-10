from lakesuperior.store.ldp_rs.lmdb_triplestore import LmdbTriplestore

ts = LmdbTriplestore('/tmp/testlmdb', True)

ts.put(b's', b'1234', 'th:t')
ret = ts.get_data(b's', 'th:t')
print('Result: {}'.format(ret.decode()))
print('DB stats: {}'.format(ts.stats()))
