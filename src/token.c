#include "util.h"
#include "token.h"
#include "postings.h"
#include "database.h"

#include <stdio.h>

/**
 * 检查输入的字符（UTF-32）是否不属于索引对象
 * @param[in] ustr 输入的字符（UTF-32）
 * @return 是否是空白字符
 * @retval 0 不是空白字符
 * @retval 1 是空白字符
 */
static int
wiser_is_ignored_char(const UTF32Char ustr)
{
  switch (ustr) {
  case ' ': case '\f': case '\n': case '\r': case '\t': case '\v':
  case '!': case '"': case '#': case '$': case '%': case '&':
  case '\'': case '(': case ')': case '*': case '+': case ',':
  case '-': case '.': case '/':
  case ':': case ';': case '<': case '=': case '>': case '?': case '@':
  case '[': case '\\': case ']': case '^': case '_': case '`':
  case '{': case '|': case '}': case '~':
  case 0x3000: /* 全角空格 */
  case 0x3001: /* 、 */
  case 0x3002: /* 。 */
  case 0xFF08: /* （ */
  case 0xFF09: /* ） */
  case 0xFF01: /* ！ */
  case 0xFF0C: /* ， */
  case 0xFF1A: /* ： */
  case 0xFF1B: /* ； */
  case 0xFF1F: /* ? */
    return 1;
  default:
    return 0;
  }
}

/**
 * 将输入的字符串分割为N-gram
 * 负责从字符串中取出 N-gram ,返回词元的长度和词元首地址的指针
 * @param[in] ustr 输入的字符串（UTF-8）
 * @param[in] ustr_end 输入的字符串中最后一个字符的位置
 * @param[in] n N-gram中N的取值。建议将其设为大于1的值
 * @param[out] start 词元的起始位置
 * @return 分割出来的词元的长度
 */
static int
ngram_next(const UTF32Char *ustr, const UTF32Char *ustr_end,
           unsigned int n, const UTF32Char **start)
{
  int i;
  const UTF32Char *p;

  /* 读取时跳过文本开头的空格等字符 */
  for (; ustr < ustr_end && wiser_is_ignored_char(*ustr); ustr++) {  //在读取构成词元的字符时,我们首先跳过了文本开头的空格等不属于索引对象的字符
  }

  /* 不断取出最多包含n个字符的词元，直到遇到不属于索引对象的字符或到达了字符串的尾部 */
  for (i = 0, p = ustr; i < n && p < ustr_end
       && !wiser_is_ignored_char(*p); i++, p++) {  //在循环时既要考虑不属于索引对象的字符,还要防止指针 p 超出字符串的末尾
  }

  *start = ustr;
  return p - ustr;
}

/**
 * 为inverted_index_value分配存储空间并对其进行初始化
 * @param[in] token_id 词元编号
 * @param[in] docs_count 包含该词元的文档数
 * @return 生成的inverted_index_value
 */
static inverted_index_value *
create_new_inverted_index(int token_id, int docs_count)
{
  inverted_index_value *ii_entry;

  ii_entry = malloc(sizeof(inverted_index_value));
  if (!ii_entry) {
    print_error("cannot allocate memory for an inverted index.");
    return NULL;
  }
  ii_entry->positions_count = 0;
  ii_entry->postings_list = NULL;
  ii_entry->token_id = token_id;
  ii_entry->docs_count = docs_count;

  return ii_entry;
}

/**
 * 为倒排列表分配存储空间并对其进行并初始化
 * @param[in] document_id 文档编号
 * @return 生成的倒排列表
 */
static postings_list *
create_new_postings_list(int document_id)
{
  postings_list *pl;

  pl = malloc(sizeof(postings_list));
  if (!pl) {
    print_error("cannot allocate memory for a postings list.");
    return NULL;
  }
  pl->document_id = document_id;
  pl->positions_count = 1;
  utarray_new(pl->positions, &ut_int_icd);

  return pl;
}

/**
 * 为传入的词元创建倒排列表
 * @param[in] env 存储着应用程序运行环境的结构体
 * @param[in] document_id 文档编号
 * @param[in] token 词元（UTF-8）
 * @param[in] token_size 词元的长度（以字节为单位）
 * @param[in] position 词元出现的位置
 * @param[in,out] postings 倒排列表的数组
 * @retval 0 成功
 * @retval -1 失败
 */
int
token_to_postings_list(wiser_env *env,
                       const int document_id, const char *token,
                       const unsigned int token_size,
                       const int position,
                       inverted_index_hash **postings)
{
  postings_list *pl;
  inverted_index_value *ii_entry;
  int token_id, token_docs_count;

  token_id = db_get_token_id(
               env, token, token_size, document_id, &token_docs_count);  //获取词元对应的编号
  /*
  如果之前已将编号分配给了该词元,那么在此处获取的正是这个编号;
  反之,如果之前没有分配编号,那么函数 db_get_token_id() 会为该词元分配一个新的编号。
  */
  if (*postings) {  //如果存在已经构建好的小倒排索引
    HASH_FIND_INT(*postings, &token_id, ii_entry);  //从中获取关联到该词元编号上的倒排列表
  } else {  //如果找不到以 token_id 为键的倒排列表
    ii_entry = NULL;  //将变量 ii_entry 的值设为 NULL
  }
  if (ii_entry) {  //如果变量 ii_entry 的值不为 NULL,小倒排索引中存在关联到该词元上的倒排列表
    pl = ii_entry->postings_list;  //先将指针 pl 指向该倒排列表
    pl->positions_count++;  //然后再将该倒排列表中词元的出现次数增加 1,在计算用于对检索结果进行排名的分数时,会用到词元的出现次数
  } else {  //如果变量 ii_entry 的值为 NULL ,也就是说小倒排索引中不存在关联到该词元上的倒排列表
    ii_entry = create_new_inverted_index(token_id,
                                         document_id ? 1 : token_docs_count);  //生成一个空的小倒排索引
    if (!ii_entry) { return -1; }
    HASH_ADD_INT(*postings, token_id, ii_entry);  //将该词元添加到新建的小倒排索引中

    pl = create_new_postings_list(document_id);  //创建出了仅由 1 个文档构成的倒排列表 pl
    if (!pl) { return -1; }
    LL_APPEND(ii_entry->postings_list, pl);  //将该倒排列表添加到了刚刚生成的小倒排索引中
    /*
    此时,指针 pl 指向关联到词元上的倒排列表
    */
  }
  /* 存储位置信息 */
  utarray_push_back(pl->positions, &position);  //将词元的出现位置添加到了倒排列表中存储着出现位置的数组的末尾
  ii_entry->positions_count++;  //将当前词元在所有文档中的出现次数之和增加 1 。出现次数之和的数据存储在关联到词元的倒排列表中
  return 0;
}

/**
 * 为构成文档内容的字符串建立倒排列表的集合
 * @param[in] env 存储着应用程序运行环境的结构体
 * @param[in] document_id 文档编号。为0时表示把要查询的关键词作为处理对象
 * @param[in] text 输入的字符串
 * @param[in] text_len 输入的字符串的长度
 * @param[in] n N-gram中N的取值
 * @param[in,out] postings 倒排列表的数组（也可视作是指向小倒排索引的指针）。若传入的指针指向了NULL，
 *                         则表示要新建一个倒排列表的数组（小倒排索引）。若传入的指针指向了之前就已经存在的倒排列表的数组，
 *                         则表示要添加元素
 * @retval 0 成功
 * @retval -1 失败
 */
int
text_to_postings_lists(wiser_env *env,
                       const int document_id, const UTF32Char *text,
                       const unsigned int text_len,
                       const int n, inverted_index_hash **postings)
{
  /* FIXME: now same document update is broken. */
  int t_len, position = 0;
  const UTF32Char *t = text, *text_end = text + text_len;

  inverted_index_hash *buffer_postings = NULL;

  for (; (t_len = ngram_next(t, text_end, n, &t)); t++, position++) {  //通过调用位于 token.c 中的函数 ngram_next() ,从字符串 t 中取出了一个 N-gram ,同时还获取了词元的长度 t_len 和指向其首地址的指针 t
    /* 检索时，忽略掉由t中长度不足N-gram的最后几个字符构成的词元 */
    if (t_len >= n || document_id) {
      int retval, t_8_size;
      char t_8[n * MAX_UTF8_SIZE];

      utf32toutf8(t, t_len, t_8, &t_8_size);  //将词元的字符编码由 UTF-32 转换成了 UTF-8

      retval = token_to_postings_list(env, document_id, t_8, t_8_size,
                                      position, &buffer_postings);  //将该词元添加到倒排列表中
      if (retval) { return retval; }
    }
  }

  if (*postings) {  //如果已经存在小倒排索引了
    merge_inverted_index(*postings, buffer_postings);  //将其与刚刚构建出的倒排索引合并
  } else {  //如果还没有小倒排索引
    *postings = buffer_postings;  //将刚刚构建出的倒排索引作为小倒排索引
  }

  return 0;
}

/**
 * 打印指定的词元
 * @param[in] env 存储着应用程序运行环境的结构体
 * @param[in] token_id 词元编号
 */
void
dump_token(wiser_env *env, int token_id)
{
  int token_len;
  const char *token;

  db_get_token(env, token_id, &token, &token_len);
  printf("token: %.*s (id: %d)\n", token_len, token, token_id);
}
