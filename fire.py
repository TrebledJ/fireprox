#!/usr/bin/env python3
# from multiprocessing import Pool
from pathlib import Path
import boto3
import os
import sys
import datetime
import argparse
import configparser
from typing import Tuple, List

try:
    import words
except:
    pass

from urllib.parse import urlparse
from time import sleep
from math import ceil


DELETE_API_RATE_LIMIT_S = 30


def get_unique_domains(urls):
    domains = set()
    for url in urls:
        u = urlparse(url)
        domains.add(u.scheme + "://" + u.netloc)
    return list(domains)


class FireProx(object):
    def __init__(self, arguments: argparse.Namespace, help_text: str):
        self.profile_name = arguments.profile_name
        self.access_key = arguments.access_key
        self.secret_access_key = arguments.secret_access_key
        self.session_token = arguments.session_token
        self.region = arguments.region
        self.command = arguments.command
        self.api_id = arguments.api_id
        # self.url = arguments.url
        self.api_list = []
        self.client = None
        self.help = help_text

        if self.access_key and self.secret_access_key:
            if not self.region:
                self.error('Please provide a region with AWS credentials')

        if not self.load_creds():
            self.error('Unable to load AWS credentials')

        # if not self.command:
        #     self.error('Please provide a valid command')

    def __str__(self):
        return 'FireProx()'

    def _try_instance_profile(self) -> bool:
        """Try instance profile credentials

        :return:
        """
        try:
            if not self.region:
                self.client = boto3.client('apigateway')
            else:
                self.client = boto3.client(
                    'apigateway',
                    region_name=self.region
                )
            self.client.get_account()
            self.region = self.client._client_config.region_name
            return True
        except:
            return False

    def load_creds(self) -> bool:
        """Load credentials from AWS config and credentials files if present.

        :return:
        """
        # If no access_key, secret_key, or profile name provided, try instance credentials
        if not any([self.access_key, self.secret_access_key, self.profile_name]):
            return self._try_instance_profile()
        # Read in AWS config/credentials files if they exist
        credentials = configparser.ConfigParser()
        credentials.read(os.path.expanduser('~/.aws/credentials'))
        config = configparser.ConfigParser()
        config.read(os.path.expanduser('~/.aws/config'))
        # If profile in files, try it, but flow through if it does not work
        config_profile_section = f'profile {self.profile_name}'
        if self.profile_name in credentials:
            if config_profile_section not in config:
                print(f'Please create a section for {self.profile_name} in your ~/.aws/config file')
                return False
            if self.region is None: # Don't override region if already specified.
                self.region = config[config_profile_section].get('region', 'us-east-1')
            try:
                self.client = boto3.session.Session(profile_name=self.profile_name,
                        region_name=self.region).client('apigateway')
                self.client.get_account()
                return True
            except:
                pass
        # Maybe had profile, maybe didn't
        if self.access_key and self.secret_access_key:
            try:
                self.client = boto3.client(
                    'apigateway',
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_access_key,
                    aws_session_token=self.session_token,
                    region_name=self.region
                )
                self.client.get_account()
                self.region = self.client._client_config.region_name
                # Save/overwrite config if profile specified
                if self.profile_name:
                    if config_profile_section not in config:
                        config.add_section(config_profile_section)
                    config[config_profile_section]['region'] = self.region
                    with open(os.path.expanduser('~/.aws/config'), 'w') as file:
                        config.write(file)
                    if self.profile_name not in credentials:
                        credentials.add_section(self.profile_name)
                    credentials[self.profile_name]['aws_access_key_id'] = self.access_key
                    credentials[self.profile_name]['aws_secret_access_key'] = self.secret_access_key
                    if self.session_token:
                        credentials[self.profile_name]['aws_session_token'] = self.session_token
                    else:
                        credentials.remove_option(self.profile_name, 'aws_session_token')
                    with open(os.path.expanduser('~/.aws/credentials'), 'w') as file:
                        credentials.write(file)
                return True
            except:
                return False
        else:
            return False

    def error(self, error):
        print(self.help)
        sys.exit(error)

    @staticmethod
    def _clean_url(url):
        if url[-1] == '/':
            url = url[:-1]
        return url

    def get_template(self, urls):
        urls = [FireProx._clean_url(u) for u in urls]
        
        title = 'fireprox_{}'.format(words.get_random_word())
        version_date = f'{datetime.datetime.now():%Y-%m-%dT%XZ}'
        path = '''
        "/s-{{word}}": {
            "x-amazon-apigateway-any-method": {
                "parameters": [
                    {
                        "name": "X-My-X-Forwarded-For",
                        "in": "header",
                        "required": true,
                        "type": "string"
                    }
                ],
                "responses": {},
                "x-amazon-apigateway-integration": {
                    "uri": "{{url}}/",
                    "responses": {
                        "default": {
                            "statusCode": "200"
                        }
                    },
                    "requestParameters": {
                        "integration.request.header.X-Forwarded-For": "method.request.header.X-My-X-Forwarded-For"
                    },
                    "passthroughBehavior": "when_no_match",
                    "httpMethod": "ANY",
                    "cacheNamespace": "19gna3",
                    "type": "http_proxy"
                }
            }
        },
        "/s-{{word}}{proxy+}/": {
            "x-amazon-apigateway-any-method": {
                "parameters": [
                    {
                        "name": "proxy",
                        "in": "path",
                        "required": true,
                        "type": "string"
                    },
                    {
                        "name": "X-My-X-Forwarded-For",
                        "in": "header",
                        "required": true,
                        "type": "string"
                    }
                ],
                "responses": {},
                "x-amazon-apigateway-integration": {
                    "uri": "{{url}}/{proxy}/",
                    "responses": {
                        "default": {
                            "statusCode": "200"
                        }
                    },
                    "requestParameters": {
                        "integration.request.path.proxy": "method.request.path.proxy",
                        "integration.request.header.X-Forwarded-For": "method.request.header.X-My-X-Forwarded-For"
                    },
                    "passthroughBehavior": "when_no_match",
                    "httpMethod": "ANY",
                    "cacheNamespace": "19gna3",
                    "cacheKeyParameters": [
                        "method.request.path.proxy"
                    ],
                    "type": "http_proxy"
                }
            }
        },
        "/{{word}}": {
            "x-amazon-apigateway-any-method": {
                "parameters": [
                    {
                        "name": "X-My-X-Forwarded-For",
                        "in": "header",
                        "required": true,
                        "type": "string"
                    }
                ],
                "responses": {},
                "x-amazon-apigateway-integration": {
                    "uri": "{{url}}",
                    "responses": {
                        "default": {
                            "statusCode": "200"
                        }
                    },
                    "requestParameters": {
                        "integration.request.header.X-Forwarded-For": "method.request.header.X-My-X-Forwarded-For"
                    },
                    "passthroughBehavior": "when_no_match",
                    "httpMethod": "ANY",
                    "cacheNamespace": "19gna3",
                    "type": "http_proxy"
                }
            }
        },
        "/{{word}}{proxy+}": {
            "x-amazon-apigateway-any-method": {
                "parameters": [
                    {
                        "name": "proxy",
                        "in": "path",
                        "required": true,
                        "type": "string"
                    },
                    {
                        "name": "X-My-X-Forwarded-For",
                        "in": "header",
                        "required": true,
                        "type": "string"
                    }
                ],
                "responses": {},
                "x-amazon-apigateway-integration": {
                    "uri": "{{url}}/{proxy}",
                    "responses": {
                        "default": {
                            "statusCode": "200"
                        }
                    },
                    "requestParameters": {
                        "integration.request.path.proxy": "method.request.path.proxy",
                        "integration.request.header.X-Forwarded-For": "method.request.header.X-My-X-Forwarded-For"
                    },
                    "passthroughBehavior": "when_no_match",
                    "httpMethod": "ANY",
                    "cacheNamespace": "19gna3",
                    "cacheKeyParameters": [
                        "method.request.path.proxy"
                    ],
                    "type": "http_proxy"
                }
            }
        }
        '''
        template = '''
        {
          "swagger": "2.0",
          "info": {
            "version": "{{version_date}}",
            "title": "{{title}}"
          },
          "basePath": "/",
          "schemes": [
            "https"
          ],
          "paths": {
            {{paths}}
          }
        }
        '''

        paths = []
        ws = words.get_random_words(len(urls))
        for url, word in zip(urls, ws):
            paths += [
                path.replace('{{url}}', url).replace('{{word}}', word + '/')
            ]

        template = template.replace('{{paths}}', ',\n'.join(paths))
        template = template.replace('{{title}}', title)
        template = template.replace('{{version_date}}', version_date)

        return str.encode(template), ws

    def create_api(self, urls):
        if not urls:
            self.error('Please provide a valid URL end-point')

        if len(urls) > 1:
            print(f'Creating => {len(urls)} urls...')
        else:
            print(f'Creating => {urls[0]}...')

        template, _words = self.get_template(urls)
        response = self.client.import_rest_api(
            parameters={
                'endpointConfigurationTypes': 'REGIONAL'
            },
            body=template
        )
        resource_id, proxy_url = self.create_deployment(response['id'])
        self.store_api(
            response['id'],
            response['name'],
            response['createdDate'],
            response['version'],
            urls,
            _words,
            resource_id,
            proxy_url
        )
    
    def update_api(self, api_id, url):
        if not any([api_id, url]):
            self.error('Please provide a valid API ID and URL end-point')

        if url[-1] == '/':
            url = url[:-1]

        resource_id = self.get_resource(api_id)
        if resource_id:
            print(f'Found resource {resource_id} for {api_id}!')
            response = self.client.update_integration(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod='ANY',
                patchOperations=[
                    {
                        'op': 'replace',
                        'path': '/uri',
                        'value': '{}/{}'.format(url, r'{proxy}'),
                    },
                ]
            )
            return response['uri'].replace('/{proxy}', '') == url
        else:
            self.error(f'Unable to update, no valid resource for {api_id}')

    def delete_api(self, api_id_to_delete=None):
        if not api_id_to_delete:
            self.error('Please provide a valid API ID')

        for _, api_id, _ in self.get_api_ids():
            if api_id_to_delete == api_id:
                response = self.client.delete_rest_api(
                    restApiId=api_id
                )
                return True
        
        return False
        
    def delete_all(self):
        items = [*self.get_api_ids()]
        num_ids = len(items)

        if num_ids == 0:
            print('Nothing to delete.')
            return False
        
        if num_ids > 1:
            print('This may take a while...')
            seconds = DELETE_API_RATE_LIMIT_S * (num_ids - 1)
            m, s = seconds // 60, seconds % 60
            print(f'Estimated time: {m}min {s}s')

        for i, (_, api_id, _) in enumerate(items):
            response = self.client.delete_rest_api(
                restApiId=api_id
            )

            print(f'Deleting {api_id} => Success!')

            # Delete quota is 1 per 30s.
            if i + 1 < num_ids:
                sleep(DELETE_API_RATE_LIMIT_S + 0.5)
    
        return True

    
    def list_api(self):
        for created_dt, api_id, name in self.get_api_ids():
            if self.api_id is not None and self.api_id != api_id:
                # If api_id is provided and doesn't match... skip.
                continue

            target_urls_and_subdir = self.get_integrations(api_id)
            url_base = f'https://{api_id}.execute-api.{self.region}.amazonaws.com/fireprox/'
            for proxy_url, subdir in target_urls_and_subdir:
                url = url_base + subdir + '/'
                print(f'[{created_dt}] ({api_id}) {name}: {url} => {proxy_url}')

    def get_api_ids(self):
        response = self.client.get_rest_apis()
        for item in response['items']:
            created_dt = item['createdDate']
            api_id = item['id']
            name = item['name']
            yield (created_dt, api_id, name)

    def list_api_ids(self):
        for created_dt, api_id, name in self.get_api_ids():
            print(f'[{created_dt}] ({api_id}) {name}')

    def store_api(self, api_id, name, created_dt, version_dt, urls, _words,
                  resource_id, proxy_url):
        for url, word in zip(urls, _words):
            print(
                f'[{created_dt}] ({api_id}) {name} => {proxy_url}{word}/ ({url})'
            )

    def create_deployment(self, api_id):
        if not api_id:
            self.error('Please provide a valid API ID')

        response = self.client.create_deployment(
            restApiId=api_id,
            stageName='fireprox',
            stageDescription='FireProx Prod',
            description='FireProx Production Deployment'
        )
        resource_id = response['id']
        return (resource_id,
                f'https://{api_id}.execute-api.{self.region}.amazonaws.com/fireprox/')

    def get_resources(self, api_id):
        """Get the unique subdirectories and their ID, e.g. http://.../fireprox/foo, http://.../fireprox/bar."""
        if not api_id:
            self.error('Please provide a valid API ID')
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/apigateway/client/get_resources.html#get-resources
        response = self.client.get_resources(restApiId=api_id, limit=500)
        resources = []
        items = response['items']
        # seen = set()
        for item in items:
            item_id = item['id']
            item_path = item['path']
            
            suffix = '/{proxy+}'
            if item_path.endswith(suffix) and not item_path.startswith('/s-'):
                resources.append((item_id, item_path[:-len(suffix)]))
        return resources

    def get_integrations(self, api_id):
        """Gets a list of target (destination) URLs and subdirectories associated with an API."""
        if not api_id:
            self.error('Please provide a valid API ID')
        ress = self.get_resources(api_id)
        if len(ress) == 0:
            self.error('Could not find resources in the API')

        integrations = []
        for resource_id, resource_path in ress:
            response = self.client.get_integration(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod='ANY'
            )
            subdir = resource_path[1:]
            integrations.append((response['uri'].replace('{proxy}', ''), subdir))
        return integrations
    
    def get_api_meta(self):
        """Get the created date and name of all API IDs without requesting for individual integrations."""
        data = [(created_dt, api_id, name) for created_dt, api_id, name in self.get_api_ids()]
        return data
        
    
    def get_url_pairs(self, prefetched_metadata = None):
        """Get pairs of (proxy_urls, target_urls)."""

        if prefetched_metadata is None:
            items = self.get_api_ids()
        else:
            items = prefetched_metadata
        
        pairs = []

        for _, api_id, _ in items:
            # created_dt = item['createdDate']
            url_base = f'https://{api_id}.execute-api.{self.region}.amazonaws.com/fireprox/'

            target_urls_and_subdir = self.get_integrations(api_id)
            for target_url, subdir in target_urls_and_subdir:
                proxy_url = url_base + subdir + '/'
                pairs.append((proxy_url, target_url))
        
        return pairs


def parse_arguments() -> Tuple[argparse.Namespace, List[str], str]:
    """Parse command line arguments and return namespace

    :return: Namespace for arguments and help text as a tuple
    """
    parser = argparse.ArgumentParser(description='FireProx API Gateway Manager')
    parser.add_argument('--profile_name',
                        help='AWS Profile Name to store/retrieve credentials', type=str, default=None)
    parser.add_argument('--access_key',
                        help='AWS Access Key', type=str, default=None)
    parser.add_argument('--secret_access_key',
                        help='AWS Secret Access Key', type=str, default=None)
    parser.add_argument('--session_token',
                        help='AWS Session Token', type=str, default=None)
    parser.add_argument('--region',
                        help='AWS Region', type=str, default=None)
    parser.add_argument('--command',
                        help='Commands: list, list-id, create, delete, delete-all, update', type=str, default=None)
    parser.add_argument('--api_id',
                        help='API ID', type=str, required=False)
    parser.add_argument('--unique',
                        help='Avoid creating duplicate proxies.', action='store_true', required=False)
    parser.add_argument('--url',
                        help='URL end-point or file containing URLs per line', type=str, required=False)
    args, rest = parser.parse_known_args()
    return args, rest, parser.format_help()


def prune_urls(urls, fp, unique=False):
    print('NOTE: Currently this only adds domains as targets, not the full path.')
    print('For example, creating a proxy to http://example.com/a/b/c will only create a proxy to http://example.com.')
    print()

    # Check scheme ://.
    # new_urls = []
    no_scheme_count = 0
    for url in urls:
        if '://' not in url:
            no_scheme_count += 1
            # new_urls.append('http://' + url)
        # else:
            # new_urls.append(url)
    
    if no_scheme_count > 0:
        print('[ERR]', no_scheme_count, 'URL(s) did not have a scheme. Please specify http:// or https://.')
        sys.exit(1)
    # urls = new_urls

    bad_port_urls = []
    for url in urls:
        u = urlparse(url)
        if ':' in u.netloc:
            _, port = u.netloc.split(':')
            p = int(port)
            if p <= 1024 and p not in {80, 443}:
                bad_port_urls.append(url)
                
    if bad_port_urls:
        print('[WARN] AWS API Gateway require ports to be 80, 443, or above 1024.')
        print('Offending URLs:')
        if len(bad_port_urls) > 8:
            print('\t' + '\n\t'.join(bad_port_urls[:8]))
            print(f'\t + {len(bad_port_urls) - 8} more URLs')
        else:
            print('\t' + '\n\t'.join(bad_port_urls))

        sys.exit(1)

    # Get unique URL domains.
    oldlen = len(urls)
    urls = get_unique_domains(urls)
    newlen = len(urls)

    if oldlen != newlen:
        print(f'Merged {oldlen} urls into {newlen} domains.')

    # Remove existing domains.
    if unique:
        url_pairs = fp.get_url_pairs()
        existing_set = set()
        for _, target_url in url_pairs:
            u = urlparse(target_url)
            existing_set.add(u.netloc)
        
        new_urls = []
        for url in urls:
            u = urlparse(url)
            if u.netloc not in existing_set:
                new_urls.append(url)
        
        if len(urls) != len(new_urls):
            print(f'Pruned {len(urls) - len(new_urls)} duplicates. => {len(new_urls)} domains.')
        
        urls = new_urls

    if len(urls) == 0:
        print('Nothing to do.')
        sys.exit(0)
    
    return urls


def main():
    """Run the main program

    :return:
    """
    args, rest_args, help_text = parse_arguments()
    fp = FireProx(args, help_text)
    if args.command == 'list':
        print(f'Listing API\'s...')
        result = fp.list_api()

    elif args.command == 'list-id':
        print(f'Listing unique API IDs...')
        result = fp.list_api_ids()

    # elif args.command == 'create':
    #     urls = [args.url]
    #     urls = prune_urls(urls, fp, args.unique)
    #     result = fp.create_api(urls)

    elif args.command == 'create':
        try:
            words.get_random_word
        except NameError:
            print("No module 'words' was found. Did you forget to copy words.py?")
            sys.exit(1)
        if args.url is None:
            print('Expected url or a file containing a list of urls.')
            sys.exit(1)

        urls = [args.url]
        if (path := Path(args.url)).is_file():
            print('Found file:', args.url)
            urls = path.read_text().splitlines()
            print('Parsed', len(urls), 'urls')

        urls = prune_urls(urls, fp, args.unique)

        # Check number of URLs.
        API_GATEWAY_LIMIT = 300 - 2
        # ...minus 2 due to / 
        INTEGRATIONS_PER_URL = 5 # This is the number of integration we define per url.
        
        num_urls = len(urls)
        max_urls = API_GATEWAY_LIMIT // INTEGRATIONS_PER_URL # Round down. Can't stuff an extra if no space.
        # if len(urls) > max_urls:
        #     print(f"Error: Number of URLs ({len(urls)}) exceeded max URLs ({max_urls}).")
        #     sys.exit(1)
        num_batches = ceil(num_urls / max_urls)

        if num_batches > 1:
            print(f'Preparing to create {num_batches} batches...')
            sleep(3)

        # Auto-batch URLs.
        for batch_i in range(num_batches):
            batch = urls[max_urls * (batch_i) : max_urls * (batch_i + 1)]
            print(f'\nBatch {batch_i+1}: {len(batch)} URLs')
            result = fp.create_api(batch)
            
            # API Limits: 1 CreateRestApi call every 3 seconds. https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html#api-gateway-execution-service-limits-table
            sleep(3.5)


    elif args.command == 'delete':
        result = fp.delete_api(fp.api_id)
        success = 'Success!' if result else 'Failed!'
        print(f'Deleting {fp.api_id} => {success}')

    elif args.command == 'delete-all':
        result = fp.delete_all()
        if result:
            print(f'Deleted all APIs!')

    elif args.command == 'update':
        print(f'Updating {fp.api_id} => {args.url}...')
        result = fp.update_api(fp.api_id, args.url)
        success = 'Success!' if result else 'Failed!'
        print(f'API Update Complete: {success}')

    # elif args.command == 'serve':
    #     print(f'Fetching proxy mappings...')
    #     url_pairs = fp.get_url_pairs()
        
    #     print(f'Serving proxy server...')
    #     from server import Server
    #     svr = Server(rest_args, url_pairs)
    #     svr.run()

    else:
        print(f'[ERROR] Unsupported command: {args.command}\n')
        print(help_text)
        sys.exit(1)


if __name__ == '__main__':
    main()
