使用说明

1、在程序目录下新建一个名 target_data 的文件夹，并将核查原始数据存放在此目录下；
    原始文件的名称必须和 Base_Data.xlsx 中所设置的一致；
2、打开 Base_Data.xlsx 文件，此文件为基础配置文件，记录着核查规则，表映射关系及表头等信息；
    a) sheet "monitoring_object_and_mapping" 记录着核查规则，其中列 “监控部署状态”为是否启用核查；
        “运算符号”、“上门限”、“下门限”则为核查规则；
    b) sheet “table_mapping” 为表的映射名称；
    c) sheet "check_result_head" 为核查结果中所要保留的字段；
    d) 其他sheet为对应原始数据的表头，核查的每个原始数据均需要有一个对应的sheet才可以正常运行；
3、程序运行完后，会在 target_data 文件夹下生成核查结果 check_result.xlsx；