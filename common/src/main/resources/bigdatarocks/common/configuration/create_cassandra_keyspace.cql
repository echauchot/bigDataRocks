// For 3 servers infrastructure with 1 datacenter :
//    with replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '2'}
// For single server :
//    with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}

drop keyspace if exists bigdatarocks;

create keyspace if not exists bigdatarocks
with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}
  and durable_writes = true;

use bigdatarocks;

create table if not exists persons (
    user_name text,
    age int,
    children_count int,
    primary key ((user_name))
) ;
