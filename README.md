RouteSavvy - Urban Mobility Optimizer 🚦🚍🚶‍♂️

CS-GY 6513 Big Data Project
Team Members:

Krapa Karthik [NET ID: kk5754, NYU ID: N12039854]
Shreyansh Bhardwaj [NET ID: sb10261, NYU ID: N17664537]
Sourik Dutta [NET ID: sd5913, NYU ID: N19304628]
Dev Thakkar [NET ID: djt8795, NYU ID: N19070379]
📚 Introduction

Urban areas, especially major cities like New York City, face persistent challenges related to traffic congestion and public transportation inefficiencies. Commuters often spend countless hours each year stuck in traffic, navigating crowded transit options, or dealing with unpredictable delays. However, the increasing availability of real-time transportation data presents an unprecedented opportunity to enhance commuter experiences and optimize urban mobility.

RouteSavvy aims to revolutionize urban commuting by leveraging big data technologies to process and analyze real-time transportation data. Our system integrates traffic patterns, public transit schedules, crowd density metrics, and historical trends to provide personalized route recommendations that adapt dynamically to changing conditions. This approach ultimately helps commuters save time and reduce frustration.

Unlike existing navigation apps that rely primarily on static maps and limited real-time updates, RouteSavvy will incorporate comprehensive data streams from multiple sources to create a holistic view of the urban transportation landscape, enabling truly intelligent routing decisions.

🎯 Objective

The primary objective of this project is to build a scalable and effective Urban Mobility Optimizer that generates personalized route recommendations based on real-time traffic conditions, public transit schedules, and crowd densities. By using big data processing and real-time analytics, RouteSavvy aims to:

Reduce travel times
Avoid congested areas
Provide more informed transportation choices
Enhance overall commuter satisfaction
🛠️ Methodology

1. Data Collection and Ingestion
Data Sources:
MTA subway data
NYC Department of Transportation traffic information
Citibike availability
Weather APIs
Social media reports (e.g., Twitter feeds)
Data Streaming:
Apache Kafka will be used to handle continuous streams of data from various transportation sources.
2. Data Processing
Real-time data from Kafka will be aggregated and analyzed using Apache Spark Structured Streaming to transform raw data into actionable insights for route optimization.
3. Machine Learning / Analytics
Predictive models will be developed to forecast traffic congestion, transit delays, and crowd density.
Route optimization algorithms will consider multiple factors beyond just distance, including real-time traffic, weather, and user preferences.
4. User Interface
A mobile application will provide real-time route recommendations, delay updates, and alternative route suggestions.
Interactive maps will visualize routes, congestion areas, and transit options.
5. Evaluation
The system's performance will be evaluated based on response time, prediction accuracy, and user satisfaction to ensure an optimal commuter experience.
📈 Expected Results

A scalable and efficient system capable of processing and analyzing real-time urban mobility data.
Accurate and timely route recommendations that adapt to current traffic and transit conditions.
Enhanced commuter experience through reduced travel times and improved reliability.
🖥️ Technologies and Tools

Programming Languages: Python
Streaming Framework: Apache Kafka
Big Data Processing: PySpark
Cloud Platforms: (e.g., Azure, AWS)
Additional Libraries: (e.g., scikit-learn, pandas, matplotlib)
📝 Conclusion

By harnessing the power of big data technologies, RouteSavvy aims to revolutionize urban commuting through real-time, data-driven route optimization. The integration of diverse data sources, advanced processing frameworks, and predictive analytics will culminate in a solution that enhances the efficiency and reliability of urban transportation systems.

NOTES
how to run: 
-docker build -t spark-image -f Dockerfile.spark .
-docker-compose up -d --build       

