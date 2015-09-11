# illdb_tools

illdb_tools is a assistant tool which could help you request Federation database by execute SQL directly,it would ignore
all authorization and authentication mechanism.Thus,you could get and modify any data as long as this function was supported.

## Usage
```
illdb_tools [OPTIONS] FUNCTION {ACTION... JSON...}
```
FUNCTION={ **permission** | **game** }
Actions depend on specific function
you need provide a json like below,and what it catains depend on specific function and action

**struction of permission json**
```
{
 "grant":"scope1 scope2",
 "grant_others":"scope1 scope2",
 "disable":"scope1",
 "game":"* or namespace",
}
```

##Function

###permission

This method allow you to set user permissions directly
``` 
Actions: GET,MODIFY

GET:	game,credential
MODIFY:	a game

```




