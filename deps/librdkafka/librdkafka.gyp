{
  "targets": [
    {
      'target_name': 'librdkafka',
      'include_dirs': [ '.' ],
      'type': 'static_library',
      'sources': [
        'rdaddr.c',
        'rdcrc32.c',
        'rdgz.c',
        'rdkafka.c',
        'rdkafka_broker.c',
        'rdkafka_defaultconf.c',
        'rdkafka_msg.c',
        'rdkafka_topic.c',
        'rdlog.c',
        'rdqueue.c',
        'rdrand.c',
        'rdthread.c',
        'snappy.c'
      ],
      'xcode_settings': {
        'OTHER_CFLAGS': [
        '-undefined dynamic_lookup',
        '-fPIC',
        '-MP',
        '-DSG',
        '-Wall',
        '-O3'
        ],
        'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
        'MACOSX_DEPLOYMENT_TARGET': '10.7'
      },
      'cflags_cc': [
        '-Wall',
        '-O3'
      ],
      'cflags': [
        '-Wall',
        '-O3'
      ],
      'cflags!': ['-fno-exceptions'],
      'cflags_cc!': ['-fno-exceptions'],
      'conditions': [
        [
          'OS=="mac"',
          {
          }
        ]
      ]
    }
  ]
}
