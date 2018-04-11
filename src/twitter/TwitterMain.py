from apscheduler.schedulers.blocking import BlockingScheduler
import TwitterDB


scheduler = BlockingScheduler()
scheduler.add_job(TwitterDB.main(), 'interval', hours=1)
scheduler.start()