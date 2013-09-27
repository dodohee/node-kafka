{
  "targets": [
    {
      'target_name': 'librdkafkaBinding',
      'sources': [
        'src/kafka.cc',
      ],
      'include_dirs': [
        'deps/librdkafka'
      ],
      'dependencies': [
        'deps/librdkafka/librdkafka.gyp:librdkafka'
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
            'libraries' : ['-lpthread -lz -lc']
          }
        ],
        [
          'OS=="linux"', 
          {
            'libraries' : ['-lpthread -lz -lc -lrt']
          }
        ],
      ]
    }
  ]
}
