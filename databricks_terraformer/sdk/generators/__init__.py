# class HCLDictionaryBuilder:
#
#     def __init__(self, src_dictionary):
#         self.__src_dictionary = src_dictionary
#         self.__hcl_dictionary = {}
#
#     @classmethod
#     def builder(cls, dictionary) -> 'HCLDictionaryBuilder':
#         return cls(dictionary)
#
#     def add_optional(self, key) -> 'HCLDictionaryBuilder':
#         if key is None or self.__src_dictionary.get(key, None) is None:
#             print("unable to find key so i am not doing anything")
#             return self
#         else:
#             self.__hcl_dictionary[key] = self.__src_dictionary[key]
#             return self
#
#     def build(self):
#         return self.__hcl_dictionary
#
#
# api_response = {
#     "blocks": [
#         {"max_capacity": 10},
#         {"max_capacity": 2},
#         {"somethingelse": 10}
#     ]
# }
# api_response2 = {}
#
# blocks = [HCLDictionaryBuilder.builder(block). \
#               add_optional("max_capacity").build() for block in api_response['blocks']]
# print(blocks)
