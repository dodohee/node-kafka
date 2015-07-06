{
  "targets": [
    {
      'target_name': 'librdkafkaBinding',
      'sources': [
        'src/kafka.cc',
      ],
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
            'xcode_settings': {
              'MACOSX_DEPLOYMENT_TARGET': '10.7',
              'GCC_ENABLE_CPP_EXCEPTIONS': 'YES'
            },
            'libraries' : ['-lrdkafka -lpthread -lz -lc']
          }
        ],
        [
          'OS=="linux"', 
          {
            'libraries' : ['-lrdkafka -lpthread -lz -lc -lrt']
          }
        ],
      ]
    }
  ]
}
