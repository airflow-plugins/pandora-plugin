import logging
import requests

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class GeneralHttpHook(BaseHook):
    """
    HTTP Hook that does not required a 'pre-configured' connection.
    """

    def __init__(self,
                 method='GET',
                 url=None,
                 port=None,
                 protocol='http'):
        self.method = method
        self.url = url
        self.port = port
        self.protocol = protocol

    # headers is required to make it required
    def get_conn(self, headers):
        """
        Returns http session for use with requests
        """
        session = requests.Session()

        if headers:
            session.headers.update(headers)

        return session

    def get_url(self, endpoint):
        url = self.url

        if not url.startswith('http'):
            url = self.protocol + '://' + url

        if self.port:
            url = url + ":" + str(self.port)

        url += "/"

        if endpoint:
            url += endpoint

        return url

    def run(self, endpoint=None, data=None, headers=None, extra_options=None):
        """
        Performs the request
        :param endpoint: The relative part of the full url
        :type endpoint: string
        :param data: The parameters to be added to the POST url
        :type data: a dictionary of string key/value pairs
        :param headers: The HTTP headers to be added to the POST request
        :type headers: a dictionary of string key/value pairs
        :param extra_options: Extra options for the 'requests' library, see the
            'requests' documentation (options to modify timeout, ssl, etc.)
        :type extra_options: A dictionary of options, where key is string and value
            depends on the option that's being modified.
        """
        extra_options = extra_options or {}

        session = self.get_conn(headers)

        url = self.get_url(endpoint)

        req = None
        if self.method == 'GET':
            # GET uses params
            req = requests.Request(self.method,
                                   url,
                                   params=data,
                                   headers=headers)
        else:
            # Others use data
            req = requests.Request(self.method,
                                   url,
                                   data=data,
                                   headers=headers)

        prepped_request = session.prepare_request(req)
        logging.info("Sending '" + self.method + "' to url: " + url)
        return self.run_and_check(session, prepped_request, extra_options)

    def run_and_check(self, session, prepped_request, extra_options):
        """
        Grabs extra options like timeout and actually runs the request,
        checking for the result
        """
        extra_options = extra_options or {}

        response = session.send(
            prepped_request,
            stream=extra_options.get("stream", False),
            verify=extra_options.get("verify", False),
            proxies=extra_options.get("proxies", {}),
            cert=extra_options.get("cert"),
            timeout=extra_options.get("timeout"),
            allow_redirects=extra_options.get("allow_redirects", True)
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            # Tried rewrapping, but not supported. This way, it's possible
            # to get reason and code for failure by checking first 3 chars
            # for the code, or do a split on ':'
            logging.error("HTTP error: " + response.reason)
            if self.method != 'GET':
                # The sensor uses GET, so this prevents filling up the log
                # with the body every time the GET 'misses'.
                # That's ok to do, because GETs should be repeatable and
                # all data should be visible in the log (no post data)
                logging.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)
        return response
