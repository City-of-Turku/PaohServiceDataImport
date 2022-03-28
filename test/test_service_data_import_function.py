import sys
sys.path.append('ServiceDataImportFunctionApp/ServiceDataImportFunction')
import unittest
from unittest.mock import MagicMock
from service_data_import.ptv_importer import *

class ServiceDataImportTest(unittest.TestCase):

    def setUp(self):
        self.municipalities_response = [{'code': '001', 
                                    'names': [{'value': 'Turku', 'language': 'fi'},
                                              {'value': 'Åbo', 'language': 'sv'},
                                              {'value': 'Turku', 'language': 'en'}]},
                                   {'code': '002', 
                                    'names': [{'value': 'Naantali', 'language': 'fi'},
                                              {'value': 'Nådendal', 'language': 'sv'},
                                              {'value': 'Naantali', 'language': 'en'}]},
                                   {'code': '003', 
                                    'names': [{'value': 'Helsinki', 'language': 'fi'},
                                              {'value': 'Helsingfors', 'language': 'sv'},
                                              {'value': 'Helsinki', 'language': 'en'}]}]
        
        self.provinces_response = [ {'code': '02', 'names': [{ 'value': 'Varsinais-Suomi', 'language': 'fi' }]}]
        guid_response = {'pageCount':1,
                         'itemList': [ {'id':'1'}, {'id': '2'}]
                         }
        services_response = [{"id": '1',
          'type': 'type1',
          'subtype': 'subtype1',
          'organizations': [],
          'serviceChannels': [{'serviceChannel': {'id': '0011'}}],
          'serviceNames': [{'language': 'fi', 'value':"Aina tuoretta"},{'language': 'en', 'value':"Always fresh"}, {'language': 'sv', 'value':"Alltid frisk"}],
          'serviceDescriptions': [],
          'requirements': [],
          'targetGroups': [{'code': 'KR1', 'name': [{'language': 'fi', 'value': 'Kansalaiset'}]}],
          'serviceClasses': [],
          'lifeEvents': [],
          'areas': []},
         {'id': '2',
          'type': 'type2',
          'subtype': 'subtype2',
          'organizations': [],
          'serviceChannels': [{'serviceChannel': {'id': '0012'}}],
          'serviceNames': [{'language': 'fi', 'value':"Kotihoiva"},{'language': 'en', 'value':"Homecare"}, {'language': 'sv', 'value':"Samma på svenska"}],
          'serviceDescriptions': [],
          'requirements': [],
          'targetGroups': [{'code': 'KR1', 'name': [{'language': 'fi', 'value': 'Kansalaiset'}]}, {'code': 'KR1.2', 'name': [{'language': 'fi', 'value': 'Lapset ja lapsipaerheet'}]}],
          'serviceClasses': [],
          'lifeEvents': [],
          'areas': []}]
        
        self.service_3 = {'id': '3',
          'type': 'type2',
          'subtype': 'subtype3',
          'organizations': [],
          'serviceChannels': [{'serviceChannel': {'id': '0011'}}],
          'serviceNames': [{'language': 'fi', 'value':"Kotihoiva"},{'language': 'en', 'value':"Homecare"}, {'language': 'sv', 'value':"Samma på svenska"}],
          'serviceDescriptions': [],
          'requirements': [],
          'targetGroups': [{'code': 'KR1', 'name': [{'language': 'fi', 'value': 'Kansalaiset'}]}, {'code': 'KR1.1', 'name': [{'language': 'fi', 'value': 'Ikäihmiset'}]}],
          'serviceClasses': [],
          'lifeEvents': [],
          'areas': []}
        
        self.channels_guid_response = {'pageCount':1,
                         'itemList': [ {'id':'0011'}, {'id': '0012'}, {'id': '0013'}]
                         }
        self.channels_response = [{"id": '0011',
          'serviceChannelType': 'type1',
          'areaType': 'ServiceLocation',
          'organizationId': 'org1',
          'services': [{'service': {'id': '1'}}],
          'serviceChannelNames': [{'language': 'fi', 'value':"Kanava1"},{'language': 'en', 'value':"Always fresh"}, {'language': 'sv', 'value':"Alltid frisk"}],
          'serviceChannelDescriptions': [],
          'webPages': [],
          'emails': [],
          'phoneNumbers': [],
          'addresses': [{'streetAddress': {'street': [], 'postOffice': [], 'municipality': {'code': '001', 'name':[]}}}],
          'areas': []},
         {"id": '0012',
          'serviceChannelType': 'ServiceLocation',
          'areaType': 'areatype2',
          'organizationId': 'org1',
          'services': [{'service': {'id': '2'}}],
          'serviceChannelNames': [{'language': 'fi', 'value':"Kanava2"},{'language': 'en', 'value':"Always fresh"}, {'language': 'sv', 'value':"Alltid frisk"}],
          'serviceChannelDescriptions': [],
          'webPages': [],
          'emails': [],
          'phoneNumbers': [],
          'addresses': [{'streetAddress': {'street': [], 'postOffice': [], 'municipality': {'code': '009', 'name':[]}}}],
          'areas': []},
         {"id": '0013',
          'serviceChannelType': 'Other',
          'areaType': 'areatype3',
          'organizationId': 'org1',
          'services': [{'service': {'id': '2'}}],
          'serviceChannelNames': [{'language': 'fi', 'value':"Kanava3"},{'language': 'en', 'value':"Always fresh"}, {'language': 'sv', 'value':"Alltid frisk"}],
          'serviceChannelDescriptions': [],
          'webPages': [],
          'emails': [],
          'phoneNumbers': [],
          'addresses': [{'streetAddress': {'street': [], 'postOffice': [], 'municipality': {'code': '009', 'name':[]}}}],
          'areas': []}]
        
        mongo_response = [{'_id': None,'max': 1000 * datetime.strptime('2021-06-09T00:00.00.000Z', "%Y-%m-%dT%H:%M.%S.%fZ").timestamp()}]
        self.mongo_client_instance = MagicMock()
        self.mongo_client_instance.service_db = MagicMock()
        self.mongo_client_instance.service_db.services = MagicMock()
        self.mongo_client_instance.service_db.services.aggregate = MagicMock()
        self.mongo_client_instance.service_db.services.aggregate.return_value = mongo_response
        self.api_session_instance = MagicMock()
        self.api_session_instance.get = MagicMock()
        get_mock_0 = MagicMock()
        get_mock_0.json = MagicMock()
        get_mock_0.json.return_value = self.municipalities_response
        get_mock_1 = MagicMock()
        get_mock_1.json = MagicMock()
        get_mock_1.json.return_value = self.provinces_response
        get_mock_2 = MagicMock()
        get_mock_2.json = MagicMock()
        get_mock_2.json.return_value = guid_response
        get_mock_3 = MagicMock()
        get_mock_3.json = MagicMock()
        get_mock_3.json.return_value = services_response
        self.api_session_instance.get.side_effect = [get_mock_0, get_mock_1, get_mock_2, get_mock_3]
        self.ptv_importer = PTVImporter(self.mongo_client_instance, self.api_session_instance)

    def test_latest_update_time(self):
        lu_time = self.ptv_importer.get_latest_update_time_from_mongo('services')
        self.assertEqual(lu_time, datetime(2021, 6, 9))
        
    def test_is_suitable(self):
        service_guids = self.ptv_importer._get_all_service_guids(None)
        now = datetime.utcnow()
        raw_services = self.ptv_importer._get_services(service_guids)
        services = []
        for service in raw_services:
            parsed_service = self.ptv_importer._parse_service_info(service)
            parsed_service['lastUpdated'] = now
            services.append(parsed_service)

        self.assertEqual(len(services), 2)  
        self.assertTrue(self.ptv_importer._is_suitable_service(services[0]))
        self.assertTrue(self.ptv_importer._is_suitable_service(services[1]))
        self.assertTrue(self.ptv_importer._is_suitable_service(self.ptv_importer._parse_service_info(self.service_3)))    

    def test_get_municipalities(self):
        
        municipalities = self.ptv_importer.municipalities
        self.assertEqual(len(municipalities), 2)
        mun_1 = municipalities[0]
        mun_name_1 = mun_1['name']['fi']
        mun_code = mun_1['id']
        self.assertEqual(mun_name_1, 'Turku')
        self.assertEqual(mun_code, '001')
        mun_2 = municipalities[1]
        mun_name_2 = mun_2['name']['sv']
        self.assertEqual(mun_name_2, 'Nådendal')
        
    def test_is_suitable_channel(self):
        api_session_instance = MagicMock()
        api_session_instance.get = MagicMock()
        get_mock_6 = MagicMock()
        get_mock_6.json = MagicMock()
        get_mock_6.json.return_value = self.channels_guid_response
        get_mock_7 = MagicMock()
        get_mock_7.json = MagicMock()
        get_mock_7.json.return_value = self.channels_response
        api_session_instance.get.side_effect = [get_mock_6, get_mock_7]
        self.ptv_importer.api_session = api_session_instance

        channel_guids = self.ptv_importer._get_service_channel_ids(datetime(2021, 6, 9))
        channel_guids = list(set(channel_guids + []))
        now = datetime.utcnow()
        raw_channels = self.ptv_importer._get_service_channels(channel_guids)
        channels = []
        for channel in raw_channels:
            parsed_channel = self.ptv_importer._parse_channel_info(channel)
            parsed_channel['lastUpdated'] = now
            channels.append(parsed_channel) 

        self.assertEqual(len(channels), 3)
        channel_1 = channels[0]
        channel_name = channel_1['name']['fi']
        self.assertEqual(channel_name, 'Kanava1')
        channel_id = channel_1['serviceIds'][0]
        self.assertEqual(channel_id, '1')
        
        channels = [channel for channel in channels if self.ptv_importer._is_suitable_channel(channel)]        
        self.assertEqual(len(channels), 2)
        
if __name__ == '__main__':
    unittest.main()

