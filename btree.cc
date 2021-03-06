#include <assert.h>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) :
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize,
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique)
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) {
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) {
      return rc;
    }

    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) {
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) {
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;

      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock

  return superblock.Unserialize(buffercache,initblock);
}


ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}


ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value,vector<SIZE_T> &pointer)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);
  SIZE_T rootPtr = superblock.info.rootnode;
  if(node==superblock.info.rootnode)
  {
  pointer.push_back(rootPtr);
  }

  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
        pointer.push_back(ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value,pointer);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) {
      rc=b.GetPtr(b.info.numkeys,ptr);
      pointer.push_back(ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value,pointer);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) {
	if (op==BTREE_OP_LOOKUP) {
	  return b.GetVal(offset,value);
	}
	else {
	  // BTREE_OP_UPDATE
	  // WRITE ME
    rc = b.SetVal(offset,value);
	if (rc) { return rc; }
	return b.Serialize(buffercache,node);
	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;


  if (dt==BTREE_DEPTH_DOT) {
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) {
      } else {
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) {
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) {
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) {
      if (offset==0) {
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) {
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) {
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) {
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) {
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) {
    os << "\" ]";
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  vector<SIZE_T> pointer;
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value,pointer);
}
ERROR_T BTreeIndex::insert_Not_Full(SIZE_T Address,BTreeNode &Temp_Node,const KEY_T &key,const VALUE_T &value)
{
   Temp_Node.info.numkeys++;
   unsigned int times=Temp_Node.info.numkeys;
   unsigned int counter=0;
   ERROR_T rc;
   for (SIZE_T offset=0;offset<times-1;offset++)
   {
   KEY_T testkey;
   Temp_Node.GetKey(offset,testkey);
      if (key<testkey)
      {
        for (unsigned int i=Temp_Node.info.numkeys-1;i>offset;i--)
        //for (unsigned int i=1;i>0;i--)
        {
            KEY_T Temp_Key;
            VALUE_T Temp_Val;
            Temp_Node.GetKey(i-1,Temp_Key);
            Temp_Node.GetVal(i-1,Temp_Val);
            Temp_Node.SetKey(i,Temp_Key);
            Temp_Node.SetVal(i,Temp_Val);

        }
        Temp_Node.SetKey(offset,key);
        Temp_Node.SetVal(offset,value);
    //    Temp_Node.SetKey(1,7);
        rc=Temp_Node.Serialize(buffercache,Address);
        if(rc)
            {return rc;}
        break;

        }
        else if (key==testkey)
        {
            break;
        }

        else
        {

            counter++;
            if (counter==(Temp_Node.info.numkeys-1))
            {
            Temp_Node.SetKey(Temp_Node.info.numkeys-1,key);
            Temp_Node.SetVal(Temp_Node.info.numkeys-1,value);
            rc=Temp_Node.Serialize(buffercache,Address);
            if(rc) {return rc;}
            break;
            }

        }


   }
}

ERROR_T BTreeIndex::insert_Not_Full_Internal(SIZE_T Address,BTreeNode &Temp_Node,SIZE_T new_blockptr_leftleaf,SIZE_T new_blockptr_rightleaf,const KEY_T &key)
{

   Temp_Node.info.numkeys++;
   unsigned int times=Temp_Node.info.numkeys;
   unsigned int counter=0;
   ERROR_T rc;
   for (SIZE_T offset=0;offset<times-1;offset++)
   {
   KEY_T testkey;
   Temp_Node.GetKey(offset,testkey);
      if (key<testkey)
      {
        for (unsigned int i=Temp_Node.info.numkeys-1;i>offset;i--)
        //for (unsigned int i=1;i>0;i--)
        {
            KEY_T Temp_Key;
            SIZE_T pointer_temp;
            Temp_Node.GetKey(i-1,Temp_Key);
            Temp_Node.SetKey(i,Temp_Key);
            Temp_Node.GetPtr(i,pointer_temp);
            Temp_Node.SetPtr(i+1,pointer_temp);
        }
        Temp_Node.SetKey(offset,key);
        Temp_Node.SetPtr(offset,new_blockptr_leftleaf);
        Temp_Node.SetPtr(offset+1,new_blockptr_rightleaf);
        rc=Temp_Node.Serialize(buffercache,Address);
        if(rc)
            {return rc;}
        break;

        }
        else if (key==testkey)
        {
            break;
        }

        else
        {

            counter++;
            if (counter==(Temp_Node.info.numkeys-1))
            {
            Temp_Node.SetKey(Temp_Node.info.numkeys-1,key);
            Temp_Node.SetPtr(Temp_Node.info.numkeys-1,new_blockptr_leftleaf);
            Temp_Node.SetPtr(Temp_Node.info.numkeys,new_blockptr_rightleaf);
            rc=Temp_Node.Serialize(buffercache,Address);
            if(rc) {return rc;}
            break;
            }

        }


   }
}
ERROR_T BTreeIndex::split_Leaf(BTreeNode Node,SIZE_T &new_blockptr_leftleaf,SIZE_T &new_blockptr_rightleaf, KEY_T &Key,VALUE_T &Val)
{   ERROR_T rc;
    BTreeNode Left_Leaf;
    BTreeNode Right_Leaf;
    AllocateNode(new_blockptr_leftleaf);
    Left_Leaf = BTreeNode(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    Left_Leaf.Serialize(buffercache, new_blockptr_leftleaf);
    rc = Left_Leaf.Unserialize(buffercache,new_blockptr_leftleaf);
    if (rc) {return rc;}
    //Allocate RightLeaf
    AllocateNode(new_blockptr_rightleaf);
    Right_Leaf = BTreeNode(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    Right_Leaf.Serialize(buffercache, new_blockptr_rightleaf);
    rc = Right_Leaf.Unserialize(buffercache,new_blockptr_rightleaf);
    if (rc) {return rc;}
    for (unsigned int offset=0;offset<(Node.info.GetNumSlotsAsLeaf())/2;offset++)
    {
        KEY_T Temp_Key;
        VALUE_T Temp_Value;
        rc=Node.GetKey(offset,Temp_Key);
        if (rc){return rc;}
        rc=Node.GetVal(offset,Temp_Value);
        if(rc){return rc;}
        Left_Leaf.info.numkeys++;
        rc=Left_Leaf.SetKey(offset,Temp_Key);
        if(rc) {return rc;}
        rc=Left_Leaf.SetVal(offset,Temp_Value);
        if(rc) {return rc;}
    }
    Left_Leaf.Serialize(buffercache,new_blockptr_leftleaf);

    for (unsigned int offset=(Node.info.GetNumSlotsAsLeaf())/2;offset<Node.info.GetNumSlotsAsLeaf();offset++)
    {
        KEY_T Temp_Key;
        VALUE_T Temp_Value;
        rc=Node.GetKey(offset,Temp_Key);
        if (rc){return rc;}
        rc=Node.GetVal(offset,Temp_Value);
        if(rc){return rc;}
        Right_Leaf.info.numkeys++;
        rc=Right_Leaf.SetKey(offset-(Node.info.GetNumSlotsAsLeaf())/2,Temp_Key);
        if(rc) {return rc;}
        rc=Right_Leaf.SetVal(offset-(Node.info.GetNumSlotsAsLeaf())/2,Temp_Value);
        if(rc) {return rc;}
    }
    Right_Leaf.Serialize(buffercache, new_blockptr_rightleaf);
    rc=Node.GetKey((Node.info.GetNumSlotsAsLeaf())/2,Key);
    rc=Node.GetVal((Node.info.GetNumSlotsAsLeaf())/2,Val);
}
ERROR_T BTreeIndex::split_Internal_Full(BTreeNode Node,SIZE_T &new_blockptr_leftInternal,SIZE_T &new_blockptr_rightInternal, KEY_T &Key)
{   ERROR_T rc;
    BTreeNode Left_Internal;
    BTreeNode Right_Internal;
    AllocateNode(new_blockptr_leftInternal);
    Left_Internal = BTreeNode(BTREE_INTERIOR_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    Left_Internal.Serialize(buffercache, new_blockptr_leftInternal);
    rc = Left_Internal.Unserialize(buffercache,new_blockptr_leftInternal);
    if (rc) {return rc;}
    //Allocate RightLeaf
    AllocateNode(new_blockptr_rightInternal);
    Right_Internal = BTreeNode(BTREE_INTERIOR_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    Right_Internal.Serialize(buffercache, new_blockptr_rightInternal);
    rc = Right_Internal.Unserialize(buffercache,new_blockptr_rightInternal);
    if (rc) {return rc;}
    for (unsigned int offset=0;offset<(Node.info.GetNumSlotsAsInterior())/2;offset++)
    {
        KEY_T Temp_Key;
        SIZE_T Temp_pointer;
        rc=Node.GetKey(offset,Temp_Key);
        if (rc){return rc;}
        rc=Node.GetPtr(offset,Temp_pointer);
        Left_Internal.info.numkeys++;
        rc=Left_Internal.SetKey(offset,Temp_Key);
        rc=Left_Internal.SetPtr(offset,Temp_pointer);
        if(rc) {return rc;}
    }
        SIZE_T Temp_pointer;
        rc=Node.GetPtr((Node.info.GetNumSlotsAsInterior())/2,Temp_pointer);
        rc=Left_Internal.SetPtr((Node.info.GetNumSlotsAsInterior())/2,Temp_pointer);
        Left_Internal.Serialize(buffercache,new_blockptr_leftInternal);

    for (unsigned int offset=(Node.info.GetNumSlotsAsInterior())/2+1;offset<Node.info.GetNumSlotsAsInterior();offset++)
    {
        KEY_T Temp_Key;
        SIZE_T Temp_pointer;
        rc=Node.GetKey(offset,Temp_Key);
        if (rc){return rc;}
        rc=Node.GetPtr(offset,Temp_pointer);
        Right_Internal.info.numkeys++;
        rc=Right_Internal.SetKey(offset-Node.info.GetNumSlotsAsInterior()/2-1,Temp_Key);
        rc=Right_Internal.SetPtr(offset-Node.info.GetNumSlotsAsInterior()/2-1,Temp_pointer);
        if(rc) {return rc;}
    }
    SIZE_T Temp_point;
    rc=Node.GetPtr(Node.info.GetNumSlotsAsInterior(),Temp_point);
    rc=Right_Internal.SetPtr((Node.info.GetNumSlotsAsInterior())/2-1,Temp_point);
    Right_Internal.Serialize(buffercache, new_blockptr_rightInternal);
    rc=Node.GetKey((Node.info.GetNumSlotsAsInterior())/2,Key);
}

ERROR_T BTreeIndex::split_Internal(vector<SIZE_T> &pointer,SIZE_T &new_blockptr_leftleaf,SIZE_T &new_blockptr_rightleaf,KEY_T &key,int &flag)
{
    ERROR_T rc;
    if(flag==0)
    {
    SIZE_T Address=pointer.back();
    pointer.pop_back();
    BTreeNode Temp_Node;
    rc=Temp_Node.Unserialize(buffercache,Address);
    if(Temp_Node.info.numkeys==Temp_Node.info.GetNumSlotsAsInterior()-1)
    {
        SIZE_T rootPtr = superblock.info.rootnode;
        if(Address!=rootPtr)
        {
        insert_Not_Full_Internal(Address,Temp_Node,new_blockptr_leftleaf,new_blockptr_rightleaf,key);
        SIZE_T new_blockptr_leftInternal;
        SIZE_T new_blockptr_rightInternal;
        KEY_T Temp_Key;
        split_Internal_Full(Temp_Node,new_blockptr_leftInternal,new_blockptr_rightInternal,Temp_Key);
        split_Internal(pointer,new_blockptr_leftInternal,new_blockptr_rightInternal,Temp_Key,flag);
        }
        if(Address==rootPtr)
        {
        insert_Not_Full_Internal(Address,Temp_Node,new_blockptr_leftleaf,new_blockptr_rightleaf,key);
        SIZE_T new_blockptr_leftInternal;
        SIZE_T new_blockptr_rightInternal;
        KEY_T Temp_Key;
        split_Internal_Full(Temp_Node,new_blockptr_leftInternal,new_blockptr_rightInternal,Temp_Key);
        BTreeNode New_Root;
        New_Root=BTreeNode(BTREE_INTERIOR_NODE,superblock.info.keysize,superblock.info.valuesize, superblock.info.blocksize);
        New_Root.info.numkeys++;
        New_Root.SetKey(0,Temp_Key);
        New_Root.SetPtr(0,new_blockptr_leftInternal);
        New_Root.SetPtr(1,new_blockptr_rightInternal);
        rc = New_Root.Serialize(buffercache, superblock.info.rootnode);
        flag=1;
        return rc;
        }
    }
    else
    {
        insert_Not_Full_Internal(Address,Temp_Node,new_blockptr_leftleaf,new_blockptr_rightleaf,key);
        flag=1;
        return rc;
    }
    }
    else
    {
    return rc;
    }
}
ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  //======ALGORITHM======
  VALUE_T val;
  bool initBlock;
  vector<SIZE_T> pointer;
  //Lookup and attempt to update key
  ERROR_T retCode = LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, val,pointer);

  //cout << "Finished LookupOrUpdateInternal" << endl;

  switch(retCode) {
    //If there is no error in the update call, end function, declare update successful.
    case ERROR_NOERROR:
    //std::cout << "Key already existed, value updated."<<std::endl;
    return ERROR_INSERT;
    //If the key doesn't exist (as expected), begin insert functionality
    case ERROR_NONEXISTENT:
      //traverse to find the leaf

    BTreeNode leafNode;
    BTreeNode rootNode;
//    BTreeNode rightLeafNode;
    ERROR_T rc;
 //   SIZE_T leafPtr;
 //   SIZE_T rightLeafPtr;

    SIZE_T rootPtr = superblock.info.rootnode;
    rootNode.Unserialize(buffercache, rootPtr);
    initBlock = false;
    if (rootNode.info.numkeys != 0) {
      initBlock = true;
    }
    rootNode.Serialize(buffercache, rootPtr);
    //If no keys  existent yet...
    if(!initBlock){
      initBlock=true;
      leafNode=BTreeNode(BTREE_LEAF_NODE,superblock.info.keysize,superblock.info.valuesize, superblock.info.blocksize);
      leafNode.info.numkeys++;
      leafNode.SetKey(0,key);
      leafNode.SetVal(0,value);
      rootNode=leafNode;
      rc = rootNode.Serialize(buffercache, superblock.info.rootnode);
      if (rc)
      {
      return rc;
       }
        //Allocate a new block, and set the values to the first key spot.
     }
     if(initBlock)
     {
        SIZE_T Address=pointer.back();
        pointer.pop_back();
        BTreeNode Temp_Node;
        rc=Temp_Node.Unserialize(buffercache,Address);
        if(Temp_Node.info.numkeys==Temp_Node.info.GetNumSlotsAsLeaf()-1)
        {
        insert_Not_Full(Address,Temp_Node,key,value);
        SIZE_T new_blockptr_leftleaf;
        SIZE_T new_blockptr_rightleaf;
        SIZE_T rootPtr = superblock.info.rootnode;
        if(Address==rootPtr)
        {
        KEY_T Temp_Key;
        VALUE_T Temp_Val;
        split_Leaf(Temp_Node,new_blockptr_leftleaf,new_blockptr_rightleaf,Temp_Key,Temp_Val);
        BTreeNode New_Root;
        New_Root=BTreeNode(BTREE_INTERIOR_NODE,superblock.info.keysize,superblock.info.valuesize, superblock.info.blocksize);
        New_Root.info.numkeys++;
        New_Root.SetKey(0,Temp_Key);
        New_Root.SetPtr(0,new_blockptr_leftleaf);
        New_Root.SetPtr(1,new_blockptr_rightleaf);
        rc = New_Root.Serialize(buffercache, superblock.info.rootnode);
        }
        if(Address!=rootPtr)
        {
         KEY_T Temp_Key;
         VALUE_T Temp_Val;
         int flag=0;
         split_Leaf(Temp_Node,new_blockptr_leftleaf,new_blockptr_rightleaf,Temp_Key,Temp_Val);
         split_Internal(pointer,new_blockptr_leftleaf,new_blockptr_rightleaf,Temp_Key,flag);
        }
        }
        else
        {
            insert_Not_Full(Address,Temp_Node,key,value);
        }
}
}
}
ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
 VALUE_T val = value;
 vector<SIZE_T> pointer;
 return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, val,pointer);
}


ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit
  //
  //
  return ERROR_UNIMPL;
}


//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);

  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) {
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) {
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) {
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) {
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) {
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) {
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  ERROR_T retCode = SanityWalk(superblock.info.rootnode);
return retCode;
}
ERROR_T BTreeIndex::SanityWalk(const SIZE_T &node) const
{
BTreeNode b;
ERROR_T rc;
SIZE_T offset;
KEY_T testkey;
KEY_T tempkey;
SIZE_T ptr;
VALUE_T value;
rc = b.Unserialize(buffercache, node);
if(rc!=ERROR_NOERROR){
  return rc;
}
switch(b.info.nodetype){
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
  for(offset=0; offset<b.info.numkeys; offset++)
  {
    rc = b.GetKey(offset,testkey);
    if(rc) {return rc; }

      //If keys are not in proper size order
    if(offset+1<b.info.numkeys-1){
      rc = b.GetKey(offset+1, tempkey);
      if(tempkey < testkey){
        std::cout<<"The keys are not properly sorted!"<<std::endl;
      }
    }
    rc=b.GetPtr(offset,ptr);
    if(rc){return rc;}
    return SanityWalk(ptr);
  }
  if(b.info.numkeys>0){
    rc = b.GetPtr(b.info.numkeys, ptr);
    if(rc) { return rc; }

      return SanityWalk(ptr);
  }else{
    std::cout << "The keys on this interior node are nonexistent."<<std::endl;
    return ERROR_NONEXISTENT;
  }
  break;
  case BTREE_LEAF_NODE:
  for(offset=0; offset<b.info.numkeys;offset++)
  {
    rc = b.GetKey(offset, testkey);
    if(rc) {
      std::cout << "Leaf Node is missing key"<<std::endl;
      return rc;
    }
    rc =b.GetVal(offset, value);
    if(rc){
      std::cout << "leaf node key is missing associated value"<<std::endl;
      return rc;
    }
    if(offset+1<b.info.numkeys)
    {
      rc = b.GetKey(offset+1, tempkey);
      if(tempkey < testkey)
      {
        std::cout<<"The keys are not properly sorted!"<<std::endl;
      }
    }
  }
  break;
  default:
  return ERROR_INSANE;
  break;
}
return ERROR_NOERROR;
}



ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  ERROR_T rc;
  rc = Display(os, BTREE_DEPTH_DOT);
  return os;;
}



