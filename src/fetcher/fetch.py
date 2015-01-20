import os
import ftplib
import urllib

from src.pipeline.exceptions import InvalidSettingException
from src.pipeline.consts import ENV_FTP_HOST, ENV_FTP_USER, ENV_FTP_PASSWORD, FTP_PATHS, WEB_PATHS
from src.pipeline.settings_helper import get_var

def grab_if_needed(file_path, gconf):
    if not os.path.isfile(file_path):
        # need to download the file from source if possible
        web_paths =  gconf[WEB_PATHS] #get_var(gconf, WEB_PATHS, optional=True)
        print "web_paths=", web_paths
        if web_paths:
            print "Downloading copy from URL ..."
            _do_http(file_path, web_paths)
        elif FTP_PATHS in gconf:
            print "We do not have a local copy of %s, downloading from FTP..." % (file_path)
            ftp_host = os.environ.get(ENV_FTP_HOST)
            ftp_user = os.environ.get(ENV_FTP_USER)
            ftp_passwd = os.environ.get(ENV_FTP_PASSWORD, "")
            if not (ftp_host or ftp_user):
                raise InvalidSettingException("Need FTP HOST and Username")

            parsed_filename = os.path.basename(file_path)
            print file_path, parsed_filename
            server_path = _get_ftp_path(file_path, gconf)
            _do_ftp(ftp_host, ftp_user, ftp_passwd, server_path, parsed_filename, file_path)
    else:
        print "We already have %s - not going to server." % (file_path)


def _do_http(file_path, web_paths):
    for directory_path, url in web_paths.items():
        if directory_path in file_path:
            print "About to download {} to {}".format(url, file_path)
            parent_dir = os.path.dirname(file_path)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            return urllib.urlretrieve (url, file_path)

def _do_ftp(ftp_host, user, passwd, server_path, parsed_filename, file_path):
    print "Retrieving from %s..." % (ftp_host)
    ftp = ftplib.FTP(ftp_host)
    ftp.login(user, passwd)
    print "Downloading from directory %s..." % (server_path)
    ftp.cwd(server_path)

    try:
        parent_dir = os.path.dirname(file_path)
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)
        file_target = open(file_path, 'wb')

        ftp.retrbinary("RETR " + parsed_filename , file_target.write)
        ftp.quit()
    except Exception, e:
        raise Exception("Could not download required file %s/%s \n\n Original err: %s" % (server_path, parsed_filename, e))

    print "Download of %s/%s to %s complete!" % (server_path, parsed_filename, file_path)



def _get_ftp_path(file_path, gconf):
    base_to_ftp = gconf[FTP_PATHS]

    for base_path in base_to_ftp:
        print base_path, file_path
        if base_path in file_path:
            return base_to_ftp[base_path]

    raise InvalidSettingException("No matching FTP Path in settings")


if __name__ == '__main__':
    grab_if_needed("data/secex/MDIC_2000.rar", None)