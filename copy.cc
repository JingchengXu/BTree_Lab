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
  pointer.push_back(rootPtr);

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
      if (key<testkey || key==testkey) {
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
	} else {
	  // BTREE_OP_UPDATE
	  // WRITE ME
	  return ERROR_UNIMPL;
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
void split_Leaf(BTreeNode old_node,BTreeNode &Result,int m,int n)
{
    SIZE_T Temp_Key;
    VALUE_T Temp_Value;
    rc=old_node.GetKey(m,Temp_Key);
    if (rc){return rc;}
    rc=old_node.GetVal(m,Temp_Value);
    if(rc){return rc;}
    Result.info.numkeys++;
    rc=Result.SetKey(n,Temp_Key);
    if(rc) {return rc;}
    rc=Result.SetVal(n,Temp_Value);
    if(rc) {return rc;}
}
int split(KEY_T element,vector<SIZE_T> &pointer,const KEY_T &key, const VALUE_T &value)
{
        SIZE_T ptr=pointer.back();
        pointer.pop_back();
        BTreeNode ReadNode;
        rc=ReadNode.Unserialize(buffercache,ptr);
        SIZE_T rootPtr = superblock.info.rootnode;
        if(rootPtr==ptr)
        {
            switch(ReadNode.info.nodetype)
            {
                case BTREE_INTERIOR_NODE:
                break;
                case BTREE_LEAF_NODE:
                SIZE_T new_blockptr_leftleaf;
                SIZE_T new_blockptr_rightleaf;
                SIZE_T new_blockptr_internal;
                BTreeNode old_rootnode;
                BTreeNode Left_Leaf;
                BTreeNode Right_Leaf;
                old_rootnode.Unserialize(buffercache,rootPtr);
                //Allocate LeftLeaf
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
                int counter;
                if (rc) {return rc;}
                for (unsigned int offset=0;offset<old_rootnode.info.GetNumSlotsAsLeaf()-1;offset++)
                {
                    SIZE_T Testkey;
                    rc=old_rootnode.GetKey(offset,Testkey);
                    if(key<Testkey&&offset<=(old_rootnode.info.GetNumSlotsAsLeaf())/2)
                    {
                        for(unsigned int i=0;i<offset;i++)
                        {
                            split_Leaf(BTreeNode old_rootnode,Left_Leaf,i,i);
                        }
                        rc=Left_Leaf.SetKey(offset,key);
                        if(rc){return rc;}
                        rc=Left_Leaf.SetVal(offset,value);
                        if(rc){return rc;}
                         for(unsigned int i=offset;i<=(old_rootnode.info.GetNumSlotsAsLeaf()-1)/2;i++)
                        {
                            split_Leaf(BTreeNode old_rootnode,Left_Leaf,i,i+1);

                        }
                         for(unsigned int i=(old_rootnode.info.GetNumSlotsAsLeaf()-1)/2+1;i<=old_rootnode.info.GetNumSlotsAsLeaf()-1;i++)
                        {
                            split_Leaf(BTreeNode old_rootnode,Right_Leaf,i,i-1-(old_rootnode.info.GetNumSlotsAsLeaf()-1)/2);
                        }

                    }
                    if(key<Testkey&&offset>(old_rootnode.info.GetNumSlotsAsLeaf()-1)/2)
                    {
                        for(unsigned int i=0;i<(old_rootnode.info.GetNumSlotsAsLeaf()-1)/2;i++)
                        {
                            split_Leaf(BTreeNode old_rootnode,Left_Leaf,i,i);
                        }
                        for(unsigned int i=(old_rootnode.info.GetNumSlotsAsLeaf()-1)/2;i<offset;i++)
                        {
                            split_Leaf(BTreeNode old_rootnode,Right_Leaf,i,i-(old_rootnode.info.GetNumSlotsAsLeaf()-1)/2);
                        }
                        rc=Right_Leaf.SetKey(offset-(old_rootnode.info.GetNumSlotsAsLeaf()-1)/2,key);
                        if(rc){return rc;}
                        rc=Right_Leaf.SetVal(offset-(old_rootnode.info.GetNumSlotsAsLeaf()-1)/2,value);
                        if(rc){return rc;}
                         for(unsigned int i=offset+1;i<(old_rootnode.info.GetNumSlotsAsLeaf()-1);i++)
                        {
                            split_Leaf(BTreeNode old_rootnode,Right_Leaf,i,i+1-(old_rootnode.info.GetNumSlotsAsLeaf()-1)/2);
                        }
                    }
		 if (key>Testkey)
		{
		counter++;
		if (counter==old_rootnode.info.GetNumSlotsAsLeaf()-1)
			{
			for (unsigned int i=0;i<old_rootnode.info.GetNumSlotsAsLeaf()/2;i++)

			{
            split_Leaf(BTreeNode old_rootnode,Left_Leaf,i,i);
			}

            for (unsigned int i=(old_rootnode.info.GetNumSlotsAsLeaf())/2;i<old_rootnode.info.GetNumSlotsLeaf()-1;i++)

			{
            split_Leaf(BTreeNode old_rootnode,Right_Leaf,i,i-(old_rootnode.info.GetNumSlotsAsLeaf())/2);
			}
			rc=Right_Leaf.SetKey((old_rootnode.info.GetNumSlotsAsLeaf())/2-1,key);
            if(rc){return rc;}
            rc=Right_Leaf.SetVal((old_rootnode.info.GetNumSlotsAsLeaf())/2-1,value);
            if(rc){return rc;}
			}
		}

                }

            }
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
    SIZE_T Temp=pointer.back();
    pointer.pop_back();
    BTreeNode Temp_Node;
    rc=Temp_Node.Unserialize(buffercache,Temp);
    int counter=0;
   switch(Temp_Node.info.nodetype)
   {
  case BTREE_INTERIOR_NODE:
    break;
   case BTREE_LEAF_NODE:
   if(Temp_Node.info.numkeys==Temp_Node.info.GetNumSlotsAsLeaf()-1)
   cerr<<"Full";
   else
   {
   rc=Temp_Node.Serialize(buffercache,Temp);
   Temp_Node.info.numkeys++;
   unsigned int times=Temp_Node.info.numkeys;
   unsigned int counter=0;
   for (SIZE_T offset=0;offset<times-1;offset++)
   {
   KEY_T testkey;
   rc=Temp_Node.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey)
      {
        for (unsigned int i=Temp_Node.info.numkeys-1;i>offset;i--)
        //for (unsigned int i=1;i>0;i--)
        {
            KEY_T Temp_Key;
            VALUE_T Temp_Val;
            rc=Temp_Node.GetKey(i-1,Temp_Key);
            if(rc)
                {
                return rc;
                }
            rc=Temp_Node.GetVal(i-1,Temp_Val);
            if(rc)
                {
                return rc;
                }
            Temp_Node.SetKey(i,Temp_Key);
            Temp_Node.SetVal(i,Temp_Val);

        }
        Temp_Node.SetKey(offset,key);
        Temp_Node.SetVal(offset,value);
    //    Temp_Node.SetKey(1,7);
        rc=Temp_Node.Serialize(buffercache,Temp);
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
            rc=Temp_Node.Serialize(buffercache,Temp);
            if(rc) {return rc;}
            break;
            }

        }


   }

  }
}
  return ERROR_NOERROR;
}
}
}


ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  return ERROR_UNIMPL;
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
  // WRITE ME
  return ERROR_UNIMPL;
}



ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  return os;
}




