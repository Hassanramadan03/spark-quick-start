import uvicorn  # type: ignore
from fastapi import FastAPI  # type: ignore
from components.detections import controller as detection_router
# from components.beats import controller as beats_router
app = FastAPI()
# Include sub-routes
app.include_router(detection_router.router,
                   prefix="/detections", tags=["detections"])
# app.include_router(beats_router.router, prefix="/beats", tags=["inspections"])


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
print("Starting server... on localhost:8000")
