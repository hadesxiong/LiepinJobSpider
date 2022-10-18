import configparser, logging, sys, redis, pymysql, datetime

# 添加路径以便引入自定义组件
sys.path.append('../')

# 引入自定义组件

from components.setcmd import setcmd_jc, setcmd_jd
from components.execmd import crawl_jc,crawl_jd

from components.etlcmd import cmp_etl, rec_etl, jc_etl, jl_etl, jd_etl

from logging.handlers import TimedRotatingFileHandler

# 调度器
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_ADDED, EVENT_JOB_REMOVED, EVENT_JOB_MODIFIED, EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_SUBMITTED

# configparser实例
cp = configparser.ConfigParser()

# 读取配置文件-db
cp.read('../configs/db.cfg')
mysql_config = {
    'host': cp.get('mysql_cfg', 'host'),
    'port': int(cp.get('mysql_cfg', 'port')),
    'user': cp.get('mysql_cfg', 'user'),
    'password': cp.get('mysql_cfg', 'password'),
    'database': cp.get('mysql_cfg', 'database')
}

redis_config = {
    'host': cp.get('redis_cfg', 'host'),
    'port': int(cp.get('redis_cfg', 'port')),
    'user': '',
    'password': cp.get('redis_cfg', 'password'),
    'db': int(cp.get('redis_cfg', 'db'))
}

# 初始化数据库-redis
redis_pool = redis.ConnectionPool(host=redis_config['host'],port=redis_config['port'],db=redis_config['db'],password=redis_config['password'],decode_responses=True)
redis_db = redis.Redis(connection_pool=redis_pool)

# 定义调度器
scheduler_log = logging.getLogger('scheduler_log')
scheduler_log.setLevel(logging.DEBUG)
# 定义log_file
scheduler_log_file = TimedRotatingFileHandler('../logs/scheduler.log',when='midnight',interval=1,backupCount=90)
scheduler_log_file.suffix = '%Y_%m_%d.bak'
scheduler_log_file.setFormatter(logging.Formatter("%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s"))
# 添加handler
scheduler_log.addHandler(scheduler_log_file)

# 后台任务实例化
bak_sched = BackgroundScheduler(timezone='Asia/Shanghai')
# 前台任务实例化
block_sched = BlockingScheduler(timezone='Asia/Shanghai')

# 读取jobcard爬去初始化命令
cp.read('../configs/args.cfg')
keyword_list = cp.get('keyword', 'keyword_list').split(',')
city_list = cp.get('city', 'city_list').split(',')

job_setcmdJC = block_sched.add_job(setcmd_jc,"cron",day_of_week='mon-sun',hour=3,minute=15,kwargs={'redis_db': redis_db,'logger_obj': scheduler_log,'keyword_list': keyword_list,'city_list': city_list})

job_crwalcmdJC = block_sched.add_job(crawl_jc,"interval",seconds=300,kwargs={'redis_db': redis_db,'logger_obj': scheduler_log})

job_crwalcmdJD = block_sched.add_job(crawl_jd,"interval",seconds=30,kwargs={'redis_db': redis_db,'logger_obj': scheduler_log})

job_setcmdJD = block_sched.add_job(setcmd_jd,"interval",seconds=300,kwargs={'redis_db': redis_db,'logger_obj': scheduler_log})

job_cmpETL = block_sched.add_job(cmp_etl,"interval",seconds=30,kwargs={'mysql_config': mysql_config,'redis_db': redis_db,'logger_obj': scheduler_log})

job_recETL = block_sched.add_job(rec_etl,"interval",seconds=30,kwargs={'mysql_config': mysql_config,'redis_db': redis_db,'logger_obj': scheduler_log})

job_jcETL = block_sched.add_job(jc_etl,"interval",seconds=30,kwargs={'mysql_config': mysql_config,'redis_db': redis_db,'logger_obj': scheduler_log})

job_jlETL = block_sched.add_job(jl_etl,"interval",seconds=30,kwargs={'mysql_config': mysql_config,'redis_db': redis_db,'logger_obj': scheduler_log})

job_jdETL = block_sched.add_job(jd_etl,"interval",seconds=30,kwargs={'mysql_config': mysql_config,'redis_db': redis_db,'logger_obj': scheduler_log})

block_sched.start()
