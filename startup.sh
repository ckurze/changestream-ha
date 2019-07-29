mkdir data20
mkdir data21
mkdir data22
#/mongodb/mongodb-osx-x86_64-4.0.6/bin/mongod --fork --port 27020 --replSet testRS --dbpath ./data20 --logpath ./data20/mongod.log
#/mongodb/mongodb-osx-x86_64-4.0.6/bin/mongod --fork --port 27021 --replSet testRS --dbpath ./data21 --logpath ./data21/mongod.log
#/mongodb/mongodb-osx-x86_64-4.0.6/bin/mongod --fork --port 27022 --replSet testRS --dbpath ./data22 --logpath ./data22/mongod.log
mongod --fork --port 27020 --replSet testRS --dbpath ./data20 --logpath ./data20/mongod.log
mongod --fork --port 27021 --replSet testRS --dbpath ./data21 --logpath ./data21/mongod.log
mongod --fork --port 27022 --replSet testRS --dbpath ./data22 --logpath ./data22/mongod.log
