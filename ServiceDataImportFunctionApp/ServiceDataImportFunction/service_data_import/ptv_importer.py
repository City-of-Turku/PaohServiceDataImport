# -*- coding: utf-8 -*-
"""
Created on Wed Jun  2 09:40:06 2021

@author: joonas.itkonen
"""
import os
from pymongo import MongoClient, DESCENDING
import requests
import json
import time
import urllib
import math
import pickle
from datetime import datetime, timezone
from typing import Optional
API = "https://api.palvelutietovaranto.suomi.fi/api/v11"
municipality_names = ["Aura", "Kaarina", "Kemiönsaari", "Koski Tl", "Kustavi", "Laitila",
                      "Lieto", "Loimaa", "Marttila", "Masku", "Mynämäki", "Naantali", "Nousiainen",
                      "Oripää", "Paimio", "Parainen", "Pyhäranta", "Pöytyä", "Raisio", "Rusko",
                      "Salo", "Sauvo", "Somero", "Taivassalo", "Turku", "Uusikaupunki", "Vehmaa"]
suitable_target_groups = ['KR1', 'KR1.2']
nonsuitable_target_groups = ['KR1.1', 'KR1.3', 'KR1.4', 'KR1.5', 'KR1.6']

class PTVImporter():
    """
    A class to fetch data from PTV

    Args
    ----------
    mongo_client : MongoClient ( default None )
        MongoDB client where service data is stored

    api_session : requests.Session ( default None )
        A requests session to send requests to PTV API


    Methods
    -------
    store_to_mongo( collection: str, to_store: list )
        Store a list of elements to Mongo in collection
    
    remove_old_from_mongo( collection: str, delete_ids: list )
        Delete elements from Mongo collection

    get_latest_update_time_from_mongo( collection: str )
        Get latest update time of services or channels from Mongo
        
    update_municipalities_in_mongo( municipalities: list )
        Replace current municipality list in Mongo with a new updated one
        
    import_services()
        Run the whole process to check new or changed services in Varsinais-Suomi and update the current state in Mongo

    """
    
    def __init__(self, mongo_client: Optional[MongoClient] = None, api_session: Optional[requests.Session] = None) -> None:
        if mongo_client is None:
            self.mongo_client = MongoClient("mongodb://{}:{}@{}:{}/{}".format(
                os.environ.get("MONGO_USERNAME"),
                os.environ.get("MONGO_PASSWORD"),
                os.environ.get("MONGO_HOST"),
                os.environ.get("MONGO_PORT"),
                os.environ.get("MONGO_DB")),
                ssl=True,
                tlsInsecure=True,
                replicaSet="globaldb",
                retrywrites=False,
                maxIdleTimeMS=120000,
                appName="@{}@".format(os.environ.get("MONGO_USERNAME"))
                )
        else:
            self.mongo_client = mongo_client
        
        # Init DB api session
        if api_session is None:
            self.api_session = requests.Session()
            self.api_session.auth = None
            self.api_session.cert = None
            self.api_session.verify = True
            self.api_session.headers = {
                                      "accept":  "text/plain"
                                   }
        else:
            self.api_session = api_session
        self.municipalities = self._get_municipalities()
        self.provinces = self._get_provinces('Varsinais-Suomi')

    def _write_pickle(self, services: list) -> None:
        with open('service_data.pkl', 'wb') as output:
            pickle.dump(services, output, pickle.HIGHEST_PROTOCOL)
            
    def _read_pickle(self) -> list:
        with open('service_data.pkl', 'rb') as input:
            services_1 = pickle.load(input)
        return(services_1)
    
    def _get_provinces(self, region_name: str) -> list:
        endpoint = "/CodeList/GetAreaCodes/type/Province"
        url = API + endpoint
        response = self.api_session.get(url=url)
        vs_region = [region for region in response.json() if region_name in [language_el.get('value') for language_el in region.get('names') if language_el.get('language') == 'fi']]
        return(vs_region)
    
    def _get_municipalities(self) -> list:
        endpoint = "/CodeList/GetMunicipalityCodes"
        url = API + endpoint
        all_municipalities = []
        response = self.api_session.get(url=url)
        languages = ['en', 'fi', 'sv']
        for municipality in response.json():
            names = {'en': None, 'fi': None, 'sv': None}
            code = municipality.get('code')
            for language in languages:
                l_name = [name.get('value') for name in municipality.get('names') if name.get('language') == language]
                if len(l_name) > 0:
                    names[language] = l_name[0]
            mun = {'name': names, 'id': code}
            if mun.get('name').get('fi') in municipality_names:
                all_municipalities.append(mun)
        return(all_municipalities)

    def _get_all_service_guids(self, lu_time: Optional[datetime] = None) -> list:
        
        endpoint = "/Service?page={}"
        if lu_time is not None:
            endpoint = endpoint + "&date=" + urllib.parse.quote_plus(lu_time.strftime("%Y-%m-%dT%H:%M:%S"))
        url = API + endpoint.format("1")
        service_guids = []
        response = self.api_session.get(url=url)
        item_list = response.json().get('itemList')
        if item_list is not None and len(item_list) > 0:
            service_guids = service_guids + item_list
        page_count = response.json().get('pageCount')
        if page_count > 1:
            for page in range(2, page_count + 1):
                url = API + endpoint.format(str(page))   
                response = self.api_session.get(url=url)
                service_guids = service_guids + response.json().get('itemList')
        guids = [service_guid.get('id') for service_guid in service_guids]
        guids = list(set(guids))
        return(guids)

    def _get_all_service_guids_by_province(self, lu_time: Optional[datetime] = None, include_whole_country: bool = True) -> list:
        
        include_whole_country_str = "true" if include_whole_country else "false"
        endpoint = "/Service/area/Province/code/{}?includeWholeCountry={}&page={}"
        vs_region = self.provinces
        if lu_time is not None:
            endpoint = endpoint + "&date=" + urllib.parse.quote_plus(lu_time.strftime("%Y-%m-%dT%H:%M:%S"))
        url = API + endpoint.format(vs_region[0].get('code'), include_whole_country_str, "1")
        service_guids = []
        response = self.api_session.get(url=url)
        item_list = response.json().get('itemList')
        if item_list is not None and len(item_list) > 0:
            service_guids = service_guids + item_list
        page_count = response.json().get('pageCount')
        if page_count > 1:
            for page in range(2, page_count + 1):
                url = API + endpoint.format(vs_region[0].get('code'), include_whole_country_str, str(page))   
                response = self.api_session.get(url=url)
                service_guids = service_guids + response.json().get('itemList')
        guids = [service_guid.get('id') for service_guid in service_guids]
        guids = list(set(guids))
        return(guids)
    
    def _get_all_service_guids_by_municipalities(self, lu_time: Optional[datetime] = None, include_whole_country: bool = True) -> list:
        
        include_whole_country_str = "true" if include_whole_country else "false"
        endpoint = "/Service/area/Municipality/code/{}?includeWholeCountry={}&page={}"
        if lu_time is not None:
            endpoint = endpoint + "&date=" + urllib.parse.quote_plus(lu_time.strftime("%Y-%m-%dT%H:%M:%S"))
        municipality_codes = [mun.get('id') for mun in self.municipalities]
        service_guids = []
        for mun_code in municipality_codes:
            url = API + endpoint.format(mun_code, include_whole_country_str, "1")

            response = self.api_session.get(url=url)
            item_list = response.json().get('itemList')
            if item_list is not None and len(item_list) > 0:
                service_guids = service_guids + item_list
            page_count = response.json().get('pageCount')
            if page_count > 1:
                for page in range(2, page_count + 1):
                    url = API + endpoint.format(mun_code, include_whole_country_str, str(page))   
                    response = self.api_session.get(url=url)
                    service_guids = service_guids + response.json().get('itemList')
        guids = [service_guid.get('id') for service_guid in service_guids]
        guids = list(set(guids))
        return(guids)
                
                
    def _get_services(self, guids: list) -> list:
        endpoint = "/Service/serviceWithGD/list"
        services = []
        batches = math.ceil(len(guids)/100)
        for batch in range(0, batches):
            start_index = batch*100
            end_index = (batch+1)*100
            url = API + endpoint + "?showHeader=true&guids=" + urllib.parse.quote_plus(','.join(guids[start_index:end_index]))
            response = self.api_session.get(url=url)  
            services = services + response.json()
        return(services)
    
    
    def _get_service_channel_ids(self, lu_time: Optional[datetime] = None) -> list:
        
        endpoint = "/ServiceChannel/area/Province/code/{}?includeWholeCountry=true&page={}"
        vs_region = self.provinces
        if lu_time is not None:
            endpoint = endpoint + "&date=" + urllib.parse.quote_plus(lu_time.strftime("%Y-%m-%dT%H:%M:%S"))
        url = API + endpoint.format(vs_region[0].get('code'), "1")
        channel_guids = []
        response = self.api_session.get(url=url)
        item_list = response.json().get('itemList')
        if item_list is not None and len(item_list) > 0:
            channel_guids = channel_guids + item_list
        page_count = response.json().get('pageCount')
        if page_count > 1:
            for page in range(2, page_count + 1):
                url = API + endpoint.format(vs_region[0].get('code'), str(page)) 
                response = self.api_session.get(url=url)
                channel_guids = channel_guids + response.json().get('itemList')
        guids = [channel_guid.get('id') for channel_guid in channel_guids]
        guids = list(set(guids))
        return(guids)
           
    def _get_service_channels(self, channel_ids: list) -> list:
        endpoint = "/ServiceChannel/list"
        channels = []
        batches = math.ceil(len(channel_ids)/100)
        for batch in range(0, batches):
            start_index = batch*100
            end_index = (batch+1)*100
            url = API + endpoint + "?showHeader=true&guids=" + urllib.parse.quote_plus(','.join(channel_ids[start_index:end_index]))
            response = self.api_session.get(url=url)  
            channels = channels + response.json()
        return(channels)
                 
    def _parse_service_info(self, service: dict) -> dict:
        service_final = {}
        service_id = service.get('id')
        service_final['id'] = service_id
        service_type = service.get('type')
        service_final['type'] = service_type
        service_subtype = service.get('subType')
        service_final['subtype'] = service_subtype
        
        channel_ids = [channel.get('serviceChannel').get('id') for channel in service.get('serviceChannels')]
        service_final['channelIds'] = channel_ids
        
        organizations = service.get('organizations')
        organization_elements = []
        for organization in organizations:
            role_type = organization.get('roleType')
            if organization.get('organization') is not None:
                organization_el = organization.get('organization')
                organization_el['roleType'] = role_type
                organization_elements.append(organization_el)
            else:
                if len(organization.get('additionalInformation')) > 0:
                    org_name = organization.get('additionalInformation')[0]['value']
                    organization_el = {'name': org_name, 'id': None, 'roleType': role_type}
                    organization_elements.append(organization_el)
            
        service_final['organizations'] = organization_elements
            
        
        languages = ['en', 'fi', 'sv']
        language_division = {'en': None, 'fi': None, 'sv': None}
        service_final['name'] = language_division.copy()
        service_final['descriptions'] = language_division.copy()
        service_final['requirement'] = language_division.copy()
        service_final['targetGroups'] = language_division.copy()
        service_final['serviceClasses'] = language_division.copy()
        service_final['areas'] = language_division.copy()
        service_final['lifeEvents'] = language_division.copy()
        # Divided by language    
        for language in languages:
            names = [l_name.get('value') for l_name in [name for name in service.get('serviceNames') if name.get('language') == language]]
            names = [l_name for l_name in names if l_name is not None]
            if len(names) > 0:
                name = ' - '.join(names)
            else:
                name = None
            service_final['name'][language] = name
            descriptions = [{'value': l_description.get('value'), 'type': l_description.get('type')} for l_description in [description for description in service.get('serviceDescriptions') if description.get('language') == language]]
            descriptions = [d for d in descriptions if d['value'] is not None]
            service_final['descriptions'][language] = descriptions
            
            requirements = [l_requirement.get('value') for l_requirement in [requirement for requirement in service.get('requirements') if requirement.get('language') == language]]
            requirements = [r for r in requirements if r is not None]
            requirement = " ".join(requirements)
            service_final['requirement'][language] = requirement
            
            # Target groups
            target_groups = service.get('targetGroups')
            target_group_elements = []
            for target_group in target_groups:
                target_group_names = [l_target_group_name.get('value') for l_target_group_name in [target_group_name for target_group_name in target_group.get('name') if target_group_name.get('language') == language]]
                target_group_names = [tgn for tgn in target_group_names if tgn is not None]
                target_group_name = " ".join(target_group_names)
                target_group_code = target_group.get('code')
                target_group_el = {"name": target_group_name,
                                   "code": target_group_code}
                target_group_elements.append(target_group_el) 
            service_final['targetGroups'][language] = target_group_elements
    
            
            # Service classes
            service_classes = service.get('serviceClasses')
            service_class_elements = []
            for service_class in service_classes:
                service_class_names = [l_service_class_name.get('value') for l_service_class_name in [service_class_name for service_class_name in service_class.get('name') if service_class_name.get('language') == language]]
                service_class_names = [sc_name for sc_name in service_class_names if sc_name is not None]
                if len(service_class_names) > 0:
                    service_class_name = ' - '.join(service_class_names)
                else:
                    service_class_name = None
                service_class_descriptions = [l_service_class_description.get('value') for l_service_class_description in [service_class_description for service_class_description in service_class.get('description') if service_class_description.get('language') == language]]
                service_class_descriptions = [scd for scd in service_class_descriptions if scd is not None]
                service_class_description = " ".join(service_class_descriptions)
                service_class_code = service_class.get('code')
                service_class_el = {"name": service_class_name,
                                    "description": service_class_description,
                                    "code": service_class_code}
                service_class_elements.append(service_class_el)
            service_final['serviceClasses'][language] = service_class_elements
                
            # Areas
            areas = service.get('areas')
            area_elements = []
            for area in areas:
                area_type = area.get('type')
                if area.get('type') == 'Municipality':
                    area = area.get('municipalities')[0]
                area_code = area.get('code')
                area_names = [l_area_name.get('value') for l_area_name in [area_name for area_name in area.get('name') if area_name.get('language') == language]]
                area_names = [a_name for a_name in area_names if a_name is not None]
                if len(area_names) > 0:
                    area_name = ' - '.join(area_names)
                else:
                    area_name = None
                area_el = {"name": area_name,
                                    "type": area_type,
                                    "code": area_code}
                area_elements.append(area_el)
            service_final['areas'][language] = area_elements
            
            # Life events
            life_events = service.get('lifeEvents')
            le_elements = []
            for life_event in life_events:
                life_event_code = life_event.get('code')
                life_event_names = [l_life_event_name.get('value') for l_life_event_name in [life_event_name for life_event_name in life_event.get('name') if life_event_name.get('language') == language]]
                life_event_names = [le_name for le_name in life_event_names if le_name is not None]
                if len(life_event_names) > 0:
                    life_event_name = ' - '.join(life_event_names)
                else:
                    life_event_name = None
                life_event_el = {"name": life_event_name,
                                    "code": life_event_code}
                le_elements.append(life_event_el)
            service_final['lifeEvents'][language] = le_elements          
            
        return(service_final)


    def _parse_channel_info(self, channel: dict) -> dict:
        channel_final = {}
        channel_id = channel.get('id')
        channel_final['id'] = channel_id
        channel_type = channel.get('serviceChannelType')
        channel_final['type'] = channel_type
        area_type = channel.get('areaType')
        channel_final['areaType'] = area_type 
        organization_id = channel.get('organizationId')
        channel_final['organizationId'] = organization_id
        
        service_ids = [service.get('service').get('id') for service in channel.get('services')]
        channel_final['serviceIds'] = service_ids
        
        languages = ['en', 'fi', 'sv']
        language_division = {'en': None, 'fi': None, 'sv': None}
        channel_final['name'] = language_division.copy()
        channel_final['descriptions'] = language_division.copy()
        channel_final['webPages'] = language_division.copy()
        channel_final['emails'] = language_division.copy()
        channel_final['phoneNumbers'] = language_division.copy()
        channel_final['addresses'] = language_division.copy()
        channel_final['areas'] = language_division.copy()
        channel_final['channelUrls'] = language_division.copy()
        # Divided by language    
        for language in languages:
            names = [l_name.get('value') for l_name in [name for name in channel.get('serviceChannelNames') if name.get('language') == language]]
            names = [l_name for l_name in names if l_name is not None]
            if len(names) > 0:
                name = ' - '.join(names)
            else:
                name = None
            channel_final['name'][language] = name
            
            if channel.get('serviceChannelDescriptions') is not None:
                descriptions = [{'value': l_description.get('value'), 'type': l_description.get('type')} for l_description in [description for description in channel.get('serviceChannelDescriptions') if description.get('language') == language]]
                descriptions = [d for d in descriptions if d['value'] is not None]
            else:
                descriptions = []
            channel_final['descriptions'][language] = descriptions
            
            # Web pages
            if channel.get('webPages') is not None:
                web_pages = [l_web_page for l_web_page in [web_page for web_page in channel.get('webPages') if web_page.get('language') == language]]
            else:
                web_pages = []
            web_page_elements = []
            for web_page in web_pages:
                web_page_url = web_page.get('url')
                web_page_elements.append(web_page_url)
            channel_final['webPages'][language] = web_page_elements
                
            # Support phones
            if channel.get('supportPhones') is not None:
                support_phones = [l_support_phone for l_support_phone in [support_phone for support_phone in channel.get('supportPhones') if support_phone.get('language') == language]]
            else:
                support_phones = []
            if channel.get('phoneNumbers') is not None:
                support_phones = support_phones + [l_support_phone for l_support_phone in [support_phone for support_phone in channel.get('phoneNumbers') if support_phone.get('language') == language]]
            
            support_phone_elements = []
            for support_phone in support_phones:
                support_phone_number = support_phone.get('number')
                support_phone_prefix = support_phone.get('prefixNumber')
                support_phone_description = support_phone.get('chargeDescription')
                support_phone_charge_type = support_phone.get('serviceChargeType')  
                support_phone_el = {"number": support_phone_number,
                                   "prefixNumber": support_phone_prefix,
                                   "chargeDescription": support_phone_description,
                                   "serviceChargeType": support_phone_charge_type}
                support_phone_elements.append(support_phone_el)
            channel_final['phoneNumbers'][language] = support_phone_elements
                
            # Support emails
            if channel.get('supportEmails') is not None:
                support_emails = [l_support_email for l_support_email in [support_email for support_email in channel.get('supportEmails') if support_email.get('language') == language]]
            else:
                support_emails = []
            if channel.get('emails') is not None:
                support_emails = support_emails + [l_support_email for l_support_email in [support_email for support_email in channel.get('emails') if support_email.get('language') == language]]
            support_email_elements = []
            for support_email in support_emails:
                support_email_value = support_email.get('value')
                support_email_elements.append(support_email_value)
            channel_final['emails'][language] = support_email_elements
            
            # Addresses
            if channel.get('addresses') is not None:
                addresses = channel.get('addresses')
            else:
                addresses = []
            address_elements = []
            for address in addresses:
                address_type = address.get('type')
                address_subtype = address.get('subType')
                street_address = address.get('streetAddress')
                street_number = None
                postal_code = None
                latitude = None
                longitude = None
                street_name = None
                post_office = None
                municipality_name = None
                municipality_code = None
                if street_address is not None:
                    street_number = street_address.get('streetNumber')
                    postal_code = street_address.get('postalCode')                    
                    latitude = street_address.get('latitude')                  
                    longitude = street_address.get('longitude')
                    street_names = [street_name.get('value') for street_name in street_address.get('street') if street_name.get('language') == language]
                    street_names = [s_name for s_name in street_names if s_name is not None]
                    if len(street_names) > 0:
                        street_name = ' - '.join(street_names)
                    post_offices = [post_office.get('value') for post_office in street_address.get('postOffice') if post_office.get('language') == language]
                    post_offices = [p_office for p_office in post_offices if p_office is not None]
                    if len(post_offices) > 0:
                        post_office = ' - '.join(post_offices)
                    municipality_code = street_address.get('municipality').get('code')
                    sa_municipality_names = [municipality_name.get('value') for municipality_name in street_address.get('municipality').get('name') if municipality_name.get('language') == language]
                    sa_municipality_names = [sa_municipality_name for sa_municipality_name in sa_municipality_names if sa_municipality_name is not None]
                    if len(sa_municipality_names) > 0:
                        municipality_name = ' - '.join(sa_municipality_names)

                address_el = {"type": address_type,
                                   "subtype": address_subtype,
                                   "streetNumber": street_number,
                                   "postalCode": postal_code,
                                   "latitude": latitude,
                                   "longitude": longitude,
                                   "streetName": street_name,
                                   "postOffice": post_office,
                                   "municipalityCode": municipality_code,
                                   "municipalityName": municipality_name}
                address_elements.append(address_el)
            channel_final['addresses'][language] = address_elements
                
            # Areas
            if channel.get('areas') is not None:
                areas = channel.get('areas')
            else:
                areas = []
            area_elements = []
            for area in areas:
                area_type = area.get('type')
                if area.get('type') == 'Municipality':
                    area = area.get('municipalities')[0]
                area_code = area.get('code')
                area_names = [l_area_name.get('value') for l_area_name in [area_name for area_name in area.get('name') if area_name.get('language') == language]]
                area_names = [a_name for a_name in area_names if a_name is not None]
                if len(area_names) > 0:
                    area_name = ' - '.join(area_names)
                else:
                    area_name=None
                area_el = {"name": area_name,
                                    "type": area_type,
                                    "code": area_code}
                area_elements.append(area_el)
            channel_final['areas'][language] = area_elements
            
            # Channel Urls
            if channel.get('channelUrls') is not None:
                channel_urls = [l_channel_url for l_channel_url in [channel_url for channel_url in channel.get('channelUrls') if channel_url.get('language') == language]]
            else:
                channel_urls = []
            channel_url_elements = []
            for channel_url in channel_urls:
                channel_url_value = channel_url.get('value')
                channel_url_type = channel_url.get('type')
                channel_url_el = {"url": channel_url_value,
                                  "type": channel_url_type}                
                channel_url_elements.append(channel_url_el)
            channel_final['channelUrls'][language] = channel_url_elements            
            
        return(channel_final)
    
    def _is_suitable_service(self, service: dict) -> bool:
        service_tg_codes = [t_group.get('code') for t_group in service.get('targetGroups')['fi']]
        contains_suitable = True
        if len(service_tg_codes) > 0:
            contains_suitable = any([True for tg_code in service_tg_codes if tg_code in suitable_target_groups])
        tg_OK = contains_suitable
        
        service_areas = service.get('areas')['fi']
        province_match = True
        municipality_match = True
        if len(service_areas) > 0:
            municipality_codes = [mun.get('id') for mun in self.municipalities]
            province_codes = [pro.get('code') for pro in self.provinces]
            address_municipality_codes = [area.get('code') for area in service_areas if area.get('type') == 'Municipality']
            address_province_codes = [area.get('code') for area in service_areas if area.get('type') == 'Province' or area.get('type') == 'Region']
            province_match = any([True for pro_code in address_province_codes if pro_code in province_codes])
            municipality_match = any([True for mun_code in address_municipality_codes if mun_code in municipality_codes])
        region_OK = province_match or municipality_match
        return(tg_OK and region_OK)        
    
    def _is_suitable_channel(self, channel: dict) -> bool:
        if channel.get('type') == 'ServiceLocation':
            addresses = channel.get('addresses')['fi']
            if len(addresses) > 0:
                municipality_codes = [mun.get('id') for mun in self.municipalities]
                address_municipality_codes = [add.get('municipalityCode') for add in addresses]
                contains_region_mun = any([True for am_code in address_municipality_codes if am_code in municipality_codes])
                return(contains_region_mun)
            else:
                return(True)     
        else:
            return(True)       
            
    
    def store_to_mongo(self, collection: str, to_store: list) -> None:
        if collection == "services":
            if len(to_store) > 0:
                self.mongo_client.service_db.services.insert_many(to_store)
            print(len(to_store), "new services stored.")
        elif collection == "channels":
            if len(to_store) > 0:
                self.mongo_client.service_db.channels.insert_many(to_store)
            print(len(to_store), "new channels stored.")
        else:
            raise Exception("Collection not recognized")

    def remove_old_from_mongo(self, collection: str, delete_ids: Optional[list] = None) -> None:
        del_count = 0
        if collection == "services":
            if delete_ids:
                delete_result = self.mongo_client.service_db.services.delete_many({'id': {"$in": delete_ids}})
            else:
                delete_result = self.mongo_client.service_db.services.delete_many({})                
            del_count = delete_result.deleted_count
            print(del_count, "old services deleted.")
        elif collection == "channels":
            if delete_ids:
                delete_result = self.mongo_client.service_db.channels.delete_many({'id': {"$in": delete_ids}})
            else:
                delete_result = self.mongo_client.service_db.channels.delete_many({})                
            del_count = delete_result.deleted_count
            print(del_count, "old channels deleted.")
        else:
            raise Exception("Collection not recognized")
        
    def get_latest_update_time_from_mongo(self, collection: str) -> Optional[datetime]:
        if collection == "services":
            last_result = self.mongo_client.service_db.services.aggregate([{"$group" : {"_id" : None, "max" : {"$max" : "$lastUpdated"}}}])
            last_result = list(last_result)
            time = None
            if len(last_result) > 0 and last_result[0].get('max') is not None:
                time = datetime.fromtimestamp(last_result[0]['max']/1000)
        elif collection == "channels":
            last_result = self.mongo_client.service_db.channels.aggregate([{"$group" : {"_id" : None, "max" : {"$max" : "$lastUpdated"}}}])
            last_result = list(last_result)
            time = None
            if len(last_result) > 0 and last_result[0].get('max') is not None:
                time = datetime.fromtimestamp(last_result[0]['max']/1000)
        else:
            raise Exception("Collection not recognized")
        return(time)

    def update_municipalities_in_mongo(self, municipalities: list) -> None:

        old_municipalities = self.mongo_client.service_db.municipalities.find({})
        old_ids = [municipality.get('id') for municipality in old_municipalities]
        del_count = 0
        delete_result = self.mongo_client.service_db.municipalities.delete_many({'id': {"$in": old_ids}})
        del_count = delete_result.deleted_count
        print(del_count, "old municipalities deleted.")

        self.mongo_client.service_db.municipalities.insert_many(municipalities)
        print(len(municipalities), "municipalities stored.")
            
    def import_services(self) -> None:
        
        ## Do full refetch if it the first day of the month
        now = datetime.utcnow()
        day_number = now.day
        if now.day == 1:
            refetch = True
        else:
            refetch = False
        
        ## Get latest addition times of services from DB
        if refetch:
            services_lu_time = None
            channels_lu_time = None
        else:
            services_lu_time = self.get_latest_update_time_from_mongo('services')
            channels_lu_time = self.get_latest_update_time_from_mongo('channels')
        
        ## Fetch new services
        service_guids = self._get_all_service_guids(services_lu_time)
        now = datetime.utcnow()
        raw_services = self._get_services(service_guids)
        services = []
        for service in raw_services:
            parsed_service = self._parse_service_info(service)
            parsed_service['lastUpdated'] = now
            services.append(parsed_service)
        
        # Filter in services that belong to suitable target groups
        services = [service for service in services if self._is_suitable_service(service)]

        ## Find out channels that are related to fetched services
        channels_ids = [service_el.get('channelIds') for service_el in services]
        channels_ids = [item for sublist in channels_ids for item in sublist]
        channels_ids = list(set(channels_ids))
        
        channel_guids = self._get_service_channel_ids(channels_lu_time)
        channel_guids = list(set(channel_guids + channels_ids))
        now = datetime.utcnow()
        raw_channels = self._get_service_channels(channel_guids)
        channels = []
        for channel in raw_channels:
            parsed_channel = self._parse_channel_info(channel)
            parsed_channel['lastUpdated'] = now
            channels.append(parsed_channel)        
        
        # Filter out channels that are service locations that are not inside region
        channels = [channel for channel in channels if self._is_suitable_channel(channel)]

        if refetch:
            self.remove_old_from_mongo('services')
            self.remove_old_from_mongo('channels')
        else:
            services_to_delete = [service.get('id') for service in services]
            channels_to_delete = [channel.get('id') for channel in channels]
            self.remove_old_from_mongo('services', services_to_delete)
            self.remove_old_from_mongo('channels', channels_to_delete)
   
        self.store_to_mongo('services', services)
        self.store_to_mongo('channels', channels)

        # Update municipalities
        municipalities = self.municipalities
        self.update_municipalities_in_mongo(municipalities)
        
        