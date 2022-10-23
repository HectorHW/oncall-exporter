import logging
import signal
import time
from environs import Env
import requests
import prometheus_client
from prometheus_client import Counter, Gauge, start_http_server
import sys


prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)

env = Env()


class Config:
    oncall_exporter_api_url = env("ONCALL_EXPORTER_API_URL")

    http_server_ip = env("HTTP_SERVER_IP", "0.0.0.0")
    http_server_port = env.int("HTTP_SERVER_PORT", 8081)
    update_period = env.int("ONCALL_EXPORTER_UPDATE_PERIOD", 10)
    timeout = env.int("ONCALL_EXPORTER_API_TIMEOUT", 5)
    oncall_exporter_log_level = env.log_level(
        "ONCALL_EXPORTER_LOG_LEVEL", logging.INFO)


def terminate(signal, frame):
    print("Terminating")
    sys.exit(0)


signal.signal(signal.SIGINT, terminate)

updaters = []


def updater(F):
    updaters.append(F)
    return F


ONCALL_API_REQUESTS_TOTAL = Counter(
    "oncall_api_requests_total", "Total count of requests to oncall API")

ONCALL_API_REQUESTS_FAILED_TOTAL = Counter(
    "oncall_api_requests_failed_total", "Total count of faled requests to oncall API")

ONCALL_USERS_TOTAL = Gauge(
    "oncall_users_total", "total number of users registered in the system"
)

ONCALL_USERS_WITHOUT_CONTACTS_GAUGE = Gauge(
    "oncall_users_without_contacts_gauge",
    "total number of users without contact data")

ONCALL_USERS_WITHOUT_PHONE_NUMBER = Gauge(
    "oncall_users_without_phone",
    "Number of users without phone data (call or sms)"
)


def request_with_counting(path: str) -> requests.Response:
    ONCALL_API_REQUESTS_TOTAL.inc()
    logging.debug(f"Requesting {path}")
    resp = requests.get(
        f"{Config.oncall_exporter_api_url}{path}", timeout=Config.timeout)
    if resp.status_code != 200:
        logging.warn(f"Request to {path} failed")
        ONCALL_API_REQUESTS_FAILED_TOTAL.inc()
    return resp


ONCALL_HEALTH_STATUS = Gauge(
    "oncall_health_status", "indicates if oncall is reachable at it's mainpage")


@updater
def health():
    resp = request_with_counting("/")
    if resp.status_code == 200:
        ONCALL_HEALTH_STATUS.set(1)
    else:
        ONCALL_HEALTH_STATUS.set(0)


@updater
def number_of_users_without_contacts():
    resp = request_with_counting("/api/v0/users")
    if resp.status_code != 200:
        return
    users = resp.json()
    logging.debug(f"Got number of users: {len(users)}")
    ONCALL_USERS_TOTAL.set(len(users))
    failed_users = 0
    no_phone = 0
    for user in users:
        if not user['contacts']:
            logging.debug(f"Found user without contacts: {user['name']}")
            failed_users += 1

        if "call" not in user['contacts'] and "sms" not in user['contacts']:
            logging.debug(f"User {user['name']} does not have phone number")
            no_phone += 1

    ONCALL_USERS_WITHOUT_CONTACTS_GAUGE.set(failed_users)
    ONCALL_USERS_WITHOUT_PHONE_NUMBER.set(no_phone)


ONCALL_TEAMS_TOTAL = Gauge(
    "oncall_teams_total", "total number of active teams present in the system"
)

ONCALL_TEAMS_UNDERSTAFFED = Gauge(
    "oncall_teams_understaffed",
    "total number of teams that do not have at least two members for current or next rotation"
)

ONCALL_TEAM_ROTATION_STAFF_COUNT = Gauge(
    "oncall_team_rotation_staff_count",
    "number of members in given team on select rotation (if rotation does not exist returns 0)",
    ["team_name", "rotation"]
)

ONCALL_TEAMS_TOTAL_UNREACHABLE_BY_PHONE = Gauge(
    "oncall_teams_total_unreachable_by_phone",
    "total number of teams that do not have phone contact data for current or next rotation"
)

ONCALL_TEAM_ROTATION_UNREACHABLE_BY_PHONE_COUNT = Gauge(
    "oncall_team_unreachable_by_phone_count",
    "number of members in given team on select rotation that do not have phone contact data",
    ["team_name", "rotation"]
)


@updater
def teams():
    resp = request_with_counting("/api/v0/teams")
    if resp.status_code != 200:
        return
    teams = resp.json()
    logging.debug(f"Got number of teams: {len(teams)}")
    ONCALL_TEAMS_TOTAL.set(len(teams))

    understaffed = 0
    unreachable = 0
    for team_name in teams:
        team_data = request_with_counting(f"/api/v0/teams/{team_name}/summary")
        if team_data.status_code != 200:
            continue
        team_data = team_data.json()
        is_understaffed = False
        is_unreachable_by_phone = False
        for event_time in ["current", "next"]:
            event = team_data[event_time]
            # team may have empty current or next event
            if not event:
                ONCALL_TEAM_ROTATION_STAFF_COUNT\
                    .labels(team_name, event_time).set(0)
                ONCALL_TEAM_ROTATION_UNREACHABLE_BY_PHONE_COUNT\
                    .labels(team_name, event_time).set(0)
                continue

            members = event["primary"] + event["secondary"]

            members_without_number = [member for member in members
                                      if "call" not in member['user_contacts']
                                      and "sms" not in member['user_contacts']]

            ONCALL_TEAM_ROTATION_STAFF_COUNT\
                .labels(team_name, event_time).set(len(members))
            ONCALL_TEAM_ROTATION_UNREACHABLE_BY_PHONE_COUNT\
                .labels(team_name, event_time).set(len(members_without_number))

            if len(members) < 2:
                is_understaffed = True
            if members_without_number == members:
                is_unreachable_by_phone = True

        if is_understaffed:
            understaffed += 1
        if is_unreachable_by_phone:
            unreachable += 1

    ONCALL_TEAMS_UNDERSTAFFED.set(understaffed)
    ONCALL_TEAMS_TOTAL_UNREACHABLE_BY_PHONE.set(unreachable)


if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%H:%M:%S',
                        level=Config.oncall_exporter_log_level)

    start_http_server(port=Config.http_server_port, addr=Config.http_server_ip)
    while True:
        for func in updaters:
            try:
                func()
            except Exception as e:
                logging.exception(e)
        time.sleep(Config.update_period)
