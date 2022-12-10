#!/usr/bin/python
# -*- coding: UTF-8 -*-


import sys
import getopt
import time
from confluent_kafka import Producer

# 需要先配置producer_conf
# producer_conf
producer_conf = {
    'bootstrap.servers': '127.0.0.1:9091'

    # ssl config
    # 'ssl.ca.location': '/etc/xxx/certs/kafka/ca-cert',
    # 'ssl.key.location': '/etc/xxx/certs/kafka/kafka.client.key',
    # 'ssl.certificate.location': '/etc/xxx/certs/kafka/kafka.client.pem',
    # 'security.protocol': 'SSL'
}
# 需要先配置producer_conf

# args
# kafka topic
kafka_topic = ''
# data file
data_file_name = 'data.json'
# send gap , s
send_gap = 0

# 统计 counter
send_success_count = 0


# func : main
def main(argv):
    read_args(argv)
    check_args()

    # create producer
    try:
        producer = Producer(**producer_conf)
    except Exception as exp:
        print('Exception ' + str(exp))
        sys.exit(2)

    print('[ARG] ===> topic : ', kafka_topic)
    print('[ARG] ===> input data file : ', data_file_name)
    print('[ARG] ===> send gap (s): ', send_gap)
    print('\n')

    count = 0
    start_time = time.time()

    # read data from file and send them
    with open(data_file_name, 'r', encoding='utf-8') as f:
        for line in f:
            data = line.strip()
            if len(data) != 0:
                delivery(producer, data)
                count = count + 1

    if count > 0:
        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        producer.flush()

    print('\n')
    print('[DONE] ===> Send : ', send_success_count,
          '/', count, ' (Success/Total)')
    print('[DONE] ===> Time : {:.2f}s'.format(time.time() - start_time))


# func : show help
def show_help():
    print('参数:')
    print('\t-i , --inputFile \n\t\t数据文件的路径,默认为data.json')
    print('\t-t , --topic \n\t\t指定发送到kafka的topic,无默认值,必填')
    print('\t-s , --sendGap \n\t\t发送间隔,单位秒,默认无间隔')
    print('\t-h \n\t\t显示参数提示')
    print('注意: 第一次使用前请配置脚本内的producer_conf!')


# func : read args
def read_args(argv):
    global data_file_name
    global kafka_topic
    global send_gap

    try:
        opts, args = getopt.getopt(
            argv, "hi:t:s:", ["inputFile=", "topic=", "sendGap"])
    except getopt.GetoptError as exp:
        print('Exception ' + str(exp))
        show_help()
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            show_help()
            sys.exit()
        elif opt in ("-i", "--inputFile"):
            data_file_name = arg
        elif opt in ("-t", "--topic"):
            kafka_topic = arg
        elif opt in ("-s", "--sendGap"):
            send_gap = float(arg)


# func : check_args
def check_args():
    if kafka_topic is None or len(kafka_topic) == 0:
        print('need arg : -t')
        sys.exit(2)
    if send_gap < 0:
        print('args error: -s')
        sys.exit(2)


# func : callback
def delivery_callback(err, msg):
    global send_success_count
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('[CALLBACK] => failed: {}'.format(err))
    else:
        print('[CALLBACK] => success. topic:[{}] , partition:[{}]'.format(
            msg.topic(), msg.partition()))
        send_success_count = send_success_count + 1


# func : send data
def delivery(producer, data):
    try:
        print('[SEND] => ', data)
        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)
        # Asynchronously produce a message. The delivery report callback will
        # be triggered from the call to poll() above, or flush() below, when the
        # message has been successfully delivered or failed permanently.
        producer.produce(kafka_topic, data.encode(
            'utf-8'), callback=delivery_callback)

        if send_gap != 0:
            time.sleep(send_gap)

    except Exception as exp:
        print('Exception ' + str(exp))





if __name__ == "__main__":
    main(sys.argv[1:])
