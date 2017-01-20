from __future__ import absolute_import
import smtplib
from email.mime.text import MIMEText

from stratum import settings

import stratum.logger
log = stratum.logger.get_logger(u'Notify_Email')


class NOTIFY_EMAIL(object):

    def notify_start(self):
        if settings.NOTIFY_EMAIL_TO != u'':
            self.send_email(
                settings.NOTIFY_EMAIL_TO,
                u'Stratum Server Started',
                u'Stratum server has started!')

    def notify_found_block(self, worker_name):
        if settings.NOTIFY_EMAIL_TO != u'':
            text = u'%s on Stratum server found a block!' % worker_name
            self.send_email(
                settings.NOTIFY_EMAIL_TO,
                u'Stratum Server Found Block',
                text)

    def notify_dead_coindaemon(self, worker_name):
        if settings.NOTIFY_EMAIL_TO != u'':
            text = u'Coin Daemon Has Crashed Please Report' % worker_name
            self.send_email(
                settings.NOTIFY_EMAIL_TO,
                u'Coin Daemon Crashed!',
                text)

    def send_email(self, to, subject, message):
        msg = MIMEText(message)
        msg[u'Subject'] = subject
        msg[u'From'] = settings.NOTIFY_EMAIL_FROM
        msg[u'To'] = to
        try:
            s = smtplib.SMTP(settings.NOTIFY_EMAIL_SERVER)
            if settings.NOTIFY_EMAIL_USERNAME != u'':
                if settings.NOTIFY_EMAIL_USETLS:
                    s.ehlo()
                    s.starttls()
                s.ehlo()
                s.login(
                    settings.NOTIFY_EMAIL_USERNAME,
                    settings.NOTIFY_EMAIL_PASSWORD)
            s.sendmail(settings.NOTIFY_EMAIL_FROM, to, msg.as_string())
            s.quit()
        except smtplib.SMTPAuthenticationError, e:
            log.error(u'Error sending Email: %s' % e[1])
        except Exception, e:
            log.error(u'Error sending Email: %s' % e[0])
