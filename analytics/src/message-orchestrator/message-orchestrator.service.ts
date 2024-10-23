import { Injectable } from "@nestjs/common";
import { catchError, mergeMap, of, pipe } from "rxjs";
import { Kafka } from "kafkajs";

@Injectable()
export class MessageOrchestratorService {
  private kafka: Kafka;
  private producer: any;
 

  onModuleInit() {
   setTimeout(() => {
    this.load()
   }, 2000);
  }
  load(){
    this.connectToKafka();
    process.env.ENABLE_MESSAGE_BROCKER = "true";
    const getRandomFromArrayRange = (arr, min = 0) => {
      return arr[Math.floor(Math.random() * (arr.length - min) + min)];
    };
  
  //   schema = StructType([
  //     StructField("imei", StringType(), True),
  //     StructField("coordinates", StructType([
  //         StructField("lat", DoubleType(), True),
  //         StructField("lng", DoubleType(), True)
  //     ]), True),
  //     StructField("time", LongType(), True),
  //     StructField("status", IntegerType(), True),
  //     StructField("assetId", StringType(), True),
  //     StructField("crewId", StringType(), True),
  //     StructField("timestamp", StringType(), True)  # Use TimestampType if needed
  // ])
    if (process.env.ENABLE_MESSAGE_BROCKER == "true")
      setInterval(() => {
        const detectionPayload = JSON.stringify({
          imei: getRandomFromArrayRange(["50", "51", "52", "53", "54"]),
          // coordinates: [
          //   getRandomFromArrayRange([55.3096847, 55.3029847, 55.3112847]),  // latitude
          //   getRandomFromArrayRange([25.2646713, 25.2678713, 25.2614713])   // longitude
          // ],
          time: Date.now(),  // Current timestamp in milliseconds
          status: getRandomFromArrayRange([0, 1]),  // Example status values
           
          assetId: getRandomFromArrayRange([
            "65e20174b339818efc002207",
            "65e20174b339818efc002208",
            "65e20174b339818efc002209"
          ]),
          crewId: getRandomFromArrayRange([
            "65e20174b339818efc002201",
            "65e20174b339818efc002202"
          ]),
          // tripId: getRandomFromArrayRange([
          //   "65e20174b339818efc002217",
          //   "65e20174b339818efc002218"
          // ]),
          // shift: "",
          timestamp: new Date().toISOString(),  // ISO format for timestamp
        });

        this.sendMessage(detectionPayload, "detections_topic");
      }, 1000); 
  }
  async connectToKafka() {
    this.kafka = new Kafka({
      clientId: "my-app",
      brokers: ["localhost:29092"],
      retry: {
        retries: 10,
      },
      
    });
    this.producer = this.kafka.producer();
    await this.producer.connect();
  }

  sendMessage(message: string, topic?: string) {
    this.producer.send({
      topic: topic || "TRANSACTIONSTOPICRESULT",
      messages: [{ value: message }],
    })
    .then((result) => {
      // console.log(result);
    })
    .catch((err) => {
      console.log(err);
    });
    // console.log("Message sent to Kafka:", message);
  }
  onDestroy() {
    this.producer.disconnect();
    this.kafka = null;
  }
  handleErrors(error, caught) {
    console.log(error);
    return of(caught);
  }
  public getPipeLine() {
    const tracker = (prsnDetections) => {
      console.log(prsnDetections);
      return of(prsnDetections);
    };
    const errorHandler = (error, caught) => this.handleErrors(error, caught);
    const createPipe = () => pipe(mergeMap(tracker), catchError(errorHandler));
    return createPipe();
  }
}
