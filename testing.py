from data_generator.data_generator import TransactionDataGenerator
import json

generator = TransactionDataGenerator()

visa_data = generator.generate_visa_transaction()
print(json.dumps(visa_data, indent=4))