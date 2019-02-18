
__author__     = 'Rob Mitchell'
__maintainer__ = 'Rob Mitchell'
__email__      = 'rob.mitchell@gmail.com'
__version__    = '1.1.0'
__status__     = 'retired'
__license__  = 'GNU Version 3'


'''
	This script was used to migrate objects from a Grizzly Swift cluster 
	to an Icehouse Swift cluster.  See  https://releases.openstack.org/
'''


import swiftclient
import sys
import hashlib
from optparse import OptionParser


def connect_v1(auth_url,user,key):
	connection = swiftclient.Connection(authurl=auth_url,\
		user=user,\
		key=key,\
		auth_version='1.0')
	if 'AUTH_' not in connection.get_auth()[0]:
		raise Exception('AUTH_ Error')
	return connection


def connect_v2(auth_url,user,tenant,key):
	connection = swiftclient.Connection(authurl=auth_url,\
		user=user,\
		tenant_name=tenant,\
		key=key,\
		auth_version='2.0')
	if 'AUTH_' not in connection.get_auth()[0]:
		raise Exception('AUTH_ Error')
	return connection


def get_containers_list(connection):
	return [x['name'] for x in connection.get_account()[1]]


def get_objects_list(connection,container_name):
	return connection.get_container(container_name,full_listing=True)[1]


def make_container(connection,container_name):
	try:
		connection.put_container(container_name)
	except:
		raise Exception('Create container ' + container_name + ' failed.'


def migrate(from_con,to_con,container_name,object_name,object_hash):
	try:
		(headers,content) = from_con.get_object(container_name,object_name)
		if object_hash != hashlib.md5(content).hexdigest().rstrip():
			raise Exception('"From" object md5 mismatch ' + container_name + ' ' + object_name)

		to_con.put_object(container_name,object_name,content,content_length=len(content))
		(headers2,content2) = to_con.get_object(container_name,object_name)
		if headers2['etag'] != object_hash:
			raise Exception('"To" object md5 mismatch ' + container_name + ' ' + object_name)
	except Exception as e:
		sys.stderr.write(str(e)+'\n') 



if __name__ == '__main__':
	parser = OptionParser()
	usage = "usage: %prog [options] container_name"
	parser.add_option("--v1_cluster", type="string", help="v1 cluster auth URL", dest="grizzly")
	parser.add_option("--v1_user", type="string", help="v1 cluster ACCOUNT", dest="grizzly_account")
	parser.add_option("--v1_pass", type="string", help="v1 cluster PASSWORD", dest="grizzly_password")
	parser.add_option("--v2_cluster", type="string", help="v2 cluster auth URL", dest="icehouse")
	parser.add_option("--v2_user", type="string", help="v2 cluster ACCOUNT", dest="icehouse_account")
	parser.add_option("--v2_pass", type="string", help="v2 cluster PASSWORD", dest="icehouse_password")
	options,arguments = parser.parse_args()

	griz = connect_v1(options.grizzly, options.grizzly_account, options.grizzly_password)
	ice = connect_v2(options.icehouse, options.icehouse_account, options.icehouse_password)

	make_container(ice,arguments[0])

	for object in get_objects_list(griz,arguments[0]):
		migrate(griz,ice,arguments[0],object['name'],object['hash'])


