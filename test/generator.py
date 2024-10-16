import csv
import argparse
import random
import string

def genRandomString(length, characters):
    return ''.join(random.choice(characters) for _ in range(length))

def genKey(max_length, real):
    if real:
        characters = string.ascii_uppercase + string.digits
    else:
        characters = string.ascii_lowercase + string.digits
    
    length = max_length #random.randint(1, max_length)
    return genRandomString(length, characters)

def genValue(max_length):
    characters = string.ascii_letters + string.digits
    length = max_length #random.randint(1, max_length)
    return genRandomString(length, characters)

def writeKVPairs(fname, num_keys, key_max_length, value_max_length, real, vk_ratio):
    key_len = key_max_length
    value_len = value_max_length
    with open(fname, mode='w', newline='') as f:
        csv_writer = csv.writer(f)
        if (vk_ratio != 0):
            key_len = random.randint(1, key_max_length)
            value_len = vk_ratio * key_len
        for i in range(num_keys):
            #csv_writer.writerow([genKey(key_max_length), genValue(value_max_length)])
            key = genKey(key_len, real)
            value = genValue (value_len)
            f.write(key + ',' + value + '\n')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--num_keys', type=int, default=1000, help='number of key-value pairs to generate')
    parser.add_argument('--num_real_keys', type=int, default=80, help='percentage of real key-value pairs')
    parser.add_argument('--base_dir', type=str, default='.', help='where to place data')
    parser.add_argument('--real-fname', type=str, default='real')
    parser.add_argument('--fake-fname', type=str, default='fake')
    parser.add_argument('--key-max-length', type=int, default=50)
    parser.add_argument('--value-max-length', type=int, default=50)
    parser.add_argument('--num-clients', type=int, default=1)
    parser.add_argument('--vk_ratio', type=int, default=0, help='Ratio of length of value to length of key')

    args = parser.parse_args()

    num_real_keys = (int)((args.num_real_keys/100) * args.num_keys)
    num_fake_keys = args.num_keys - num_real_keys
    base_dir = args.base_dir

    data_dir = base_dir + '/data/'
    num_clients = args.num_clients
    vk_ratio = args.vk_ratio

    for i in range(num_clients):
        real_fname = data_dir + args.real_fname + str(i) + '.csv'
        fake_fname = data_dir + args.fake_fname + str(i) + '.csv'
    
        # write real kv pairs
        writeKVPairs(real_fname, num_real_keys, args.key_max_length, args.value_max_length, True, vk_ratio)
        
        # write fake kv pairs
        writeKVPairs(fake_fname, num_fake_keys, args.key_max_length, args.value_max_length, False, vk_ratio)
