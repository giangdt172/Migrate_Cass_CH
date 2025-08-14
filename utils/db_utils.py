def round_timestamp(timestamp, round_time=86400):
    timestamp = int(timestamp)
    timestamp_unit_day = timestamp / round_time
    recover_to_unit_second = int(timestamp_unit_day) * round_time
    return recover_to_unit_second


def round_number(numb, round_num=1000):
    numb = int(numb)
    unit = numb / round_num
    recover_to_numb = int(unit) * round_num
    return recover_to_numb