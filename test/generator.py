import csv
import argparse
import random
import string

def genRandomString(length, characters):
    return ''.join(random.choice(characters) for _ in range(length))

def genKey(max_length):
    characters = string.ascii_letters + string.digits
    length = random.randint(1, max_length)
    return genRandomString(length, characters)

def genValue(max_length):
    characters = string.ascii_letters + string.digits
    length = random.randint(1, max_length)
    return genRandomString(length, characters)

def writeKVPairs(fname, num_keys, key_max_length, value_max_length):
    with open(fname, mode='w', newline='') as f:
        csv_writer = csv.writer(f)
        for i in range(num_keys):
            #csv_writer.writerow([genKey(key_max_length), genValue(value_max_length)])
            f.write(genKey(key_max_length) + ',' + genValue(value_max_length) + '\n')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--num_keys', type=int, default=10, help='number of key-value pairs to generate')
    parser.add_argument('--num_real_keys', type=int, default=50, help='percentage of real key-value pairs')
    parser.add_argument('--real-fname', type=str, default='real.csv')
    parser.add_argument('--fake-fname', type=str, default='fake.csv')
    parser.add_argument('--key-max-length', type=int, default=128)
    parser.add_argument('--value-max-length', type=int, default=128)

    args = parser.parse_args()

    num_real_keys = (int)((args.num_real_keys/100) * args.num_keys)
    num_fake_keys = args.num_keys - num_real_keys

    data_dir = 'data/'

    # write real kv pairs
    writeKVPairs(data_dir+args.real_fname, num_real_keys, args.key_max_length, args.value_max_length)
    
    # write fake kv pairs
    writeKVPairs(data_dir+args.fake_fname, num_fake_keys, args.key_max_length, args.value_max_length)
