
# from fastapi import FastAPI, BackgroundTasks
# from app.main import automate_linkedin  
# from app.validate import validate 

# app = FastAPI(title="LinkedIn Scraper API")
# @app.post("/run", status_code=202)
# async def trigger_scrape(bg: BackgroundTasks):
#     bg.add_task(automate_linkedin)   
#     bg.add_task(validate)         
#     return {"message": "Scraper started"}

