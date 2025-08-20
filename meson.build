project('adms-shuffle', ['c', 'cpp'],
    version: '1.0.0',
    default_options: [
        # 'werror=true',
        'warning_level=3',
        'c_std=c11',
        'cpp_std=c++20',
        'cuda_std=c++20',

        # 'buildtype=release',
        # 'b_lto=true',
        
        'buildtype=debug',
        # 'b_sanitize=address',
    ])
    
# meson --buildtype=release -Db_lto=true build

# Disable warnings for designated initializers with base class defaults
add_project_arguments([
    '-Wconversion',
    '-Wno-pedantic',
], language: ['c', 'cpp'])

if get_option('buildtype') == 'release'
    add_project_arguments('-O3', language: ['c', 'cpp'])
endif


project_headers = []
project_sources = []
project_mains = []
project_cuda_mains = []

project_includes = []



thread_dep = dependency('threads', required : true)


cpp_compiler = meson.get_compiler('cpp')

numa_dep = cpp_compiler.find_library('numa', has_headers: ['numa.h'], required: true)


librdmapp_proj = subproject('librdmapp')
librdmapp_dep = librdmapp_proj.get_variable('librdmapp_dep')


subdir('src')
subdir('tests')


project_deps = [
    thread_dep,
    numa_dep,
    librdmapp_dep,
]


project_mains += files(
    'main.cpp',
)

project_sources += files(
)

lib = static_library('admsshuffle',
    project_sources,
    include_directories: project_includes,
    dependencies: project_deps)


fs = import('fs')

foreach prog_src : project_mains
    prog_name = fs.stem(prog_src)

    link_args = []
  
    main = executable(f'@prog_name@',
        prog_src,
        include_directories: project_includes,
        link_args: link_args,
        link_with: [lib],
        dependencies: project_deps
    )
endforeach
