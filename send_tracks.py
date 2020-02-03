from confluent_kafka import Producer

def main(tracks_file, camera_id):
    p = Producer({'bootstrap.servers': 'localhost:9094'})
    topics = {
        'det': 'DetectionTopic',
        'inst': 'InstanceTopic'
    }

    # valid_inst = [2, 11, 7, 92, 95, 149, 178]

    with open(tracks_file, 'r') as file:
        line = file.readline().strip('\n')

        while line:
            sp = line.split(';')

            # if not int(sp[0]) in valid_inst:
            #     line = file.readline().strip('\n')
            #     continue
            
            t = topics[sp[1]]
            m = sp[2].replace('$$$camera_id$$$', camera_id)

            print(f'{t}: "{m}"')

            p.poll(0)
            p.produce(t, m.encode('utf-8'))

            line = file.readline().strip('\n')

    p.flush()

if __name__ == "__main__":
    # TODO args
    main('', "")