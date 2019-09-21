import _ from 'lodash';
import Async from 'async';
import chai from 'chai';
import mongoose from 'mongoose';
import MpathPlugin from './../lib/mpath';
import sinon from 'sinon';

require('sinon-mongoose');
chai.use(require('chai-subset'));

const should = chai.should();

mongoose.Promise = global.Promise;
mongoose.set('useFindAndModify', false);
mongoose.set('useCreateIndex', true);

describe('mpath plugin', () => {
  // Utils
  const locationsToPathObject = locations =>
    locations.reduce((result, location) => {
      result[location.name] = location.path;
      return result;
    }, {});

  // Mongoose
  let dbConnection;
  let Location;
  let LocationSchema = new mongoose.Schema({ _id: String, name: String });

  LocationSchema.plugin(MpathPlugin, {
    idType: String,
    pathSeparator: '#',
    onDelete: 'REPARENT',
  });

  /*
  Sample locations
  --------------------------
  africa
  europe
    norway
    sweden
      stockholm
        skansen
  */

  let africa;
  let europe;
  let sweden;
  let stockholm;
  let skansen;
  let norway;

  const createLocations = async () => {
    africa = new Location({ _id: 'af', name: 'Africa' });
    europe = new Location({ _id: 'eu', name: 'Europe' });
    norway = new Location({ _id: 'no', name: 'Norway', parent: europe });
    sweden = new Location({ _id: 'se', name: 'Sweden', parent: europe });
    stockholm = new Location({
      _id: 'sthlm',
      name: 'Stockholm',
      parent: sweden,
    });
    skansen = new Location({
      _id: 'skansen',
      name: 'Skansen',
      parent: stockholm,
    });

    await Location.deleteMany();
    await africa.save();
    await europe.save();
    await norway.save();
    await sweden.save();
    await stockholm.save();
    await skansen.save();
  };

  // Set up the fixture
  before(async () => {
    dbConnection = await mongoose.connect('mongodb://localhost:27017/mongoose-path-tree', {
      connectTimeoutMS: 3000,
      keepAlive: 2000,
      reconnectTries: 30,
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });

    try {
      Location = mongoose.model('Location', LocationSchema);
    } catch (ex) {
      mongoose.connection.deleteModel('Location');
      Location = mongoose.model('Location', LocationSchema);
    }
  });

  beforeEach(async () => {
    await Location.deleteMany({});
    await createLocations();
  });

  describe('setup', () => {
    it('should add fields to schema (default options)', () => {
      const DefaultLocationSchema = new mongoose.Schema({ name: String });
      DefaultLocationSchema.plugin(MpathPlugin);
      const LocationModel = dbConnection.model('SomeLocation', DefaultLocationSchema);
      const schemaPaths = LocationModel.schema.paths;

      should.exist(schemaPaths.parent);
      should.exist(schemaPaths.path);
    });

    it('should add fields to schema (custom options)', async () => {
      const randomId = () => _.shuffle(_.range(0, 9)).join('');

      const CustomLocationSchema = new mongoose.Schema({
        _id: { type: String, default: randomId },
        name: String,
      });

      CustomLocationSchema.plugin(MpathPlugin, {
        idType: String,
        pathSeparator: '|',
      });
      const CustomLocationModel = mongoose.model('SomeOtherLocation', CustomLocationSchema);
      const schemaPaths = CustomLocationModel.schema.paths;

      // check parent type
      schemaPaths.parent.instance.should.eql('String');

      // check path separator
      const world = new CustomLocationModel({ name: 'World' });
      const europe = new CustomLocationModel({ name: 'Europe', parent: world });
      const sweden = new CustomLocationModel({
        name: 'Sweden',
        parent: europe,
      });

      await world.save();
      await europe.save();
      await sweden.save();

      world.path.should.equal('');
      europe.path.should.equal(`|${world._id}|`);
      sweden.path.should.equal(`|${world._id}|${europe._id}|`);
    });
  });

  describe('pre save middleware', () => {
    it("should not perform any operations when document isn't new or hasn't changed parent", async () => {
      sinon.spy(sweden.collection, 'findOne');
      sinon.spy(sweden.collection, 'updateMany');

      const pathBeforeSave = sweden.path;

      await sweden.save();

      sweden.path.should.equal(pathBeforeSave);
      sinon.assert.notCalled(sweden.collection.findOne);
      sinon.assert.notCalled(sweden.collection.updateMany);
    });

    it('should set parent', () => {
      should.not.exist(africa.parent);
      should.not.exist(europe.parent);
      norway.parent.should.equal(europe._id);
      sweden.parent.should.equal(europe._id);
      stockholm.parent.should.equal(sweden._id);
      skansen.parent.should.equal(stockholm._id);
    });

    it('should set path', () => {
      africa.path.should.equal('');
      europe.path.should.equal('');
      norway.path.should.equal('#eu#');
      sweden.path.should.equal('#eu#');
      stockholm.path.should.equal('#eu#se#');
      skansen.path.should.equal('#eu#se#sthlm#');
    });

    it('should update child paths', async () => {
      sweden.parent = africa;
      await sweden.save();

      const locations = await Location.find({});
      const pathObject = locationsToPathObject(locations);

      pathObject.should.eql({
        Africa: '',
        Europe: '',
        Norway: '#eu#',
        Sweden: '#af#',
        Stockholm: '#af#se#',
        Skansen: '#af#se#sthlm#',
      });
    });

    it('should allow empty parent when using string as ID type', async function() {
      const randomId = () => _.shuffle(_.range(0, 9)).join('');
      const LocationSchema = new mongoose.Schema({
        _id: { type: String, default: randomId },
        name: String,
      });
      LocationSchema.plugin(MpathPlugin, { idType: String });
      const LocationModel = mongoose.model('LocationWithStringAsIdType', LocationSchema);
      await LocationModel.deleteMany({});

      const world = new LocationModel({ _id: 'wo', name: 'World', parent: '' });
      await world.save();
    });
  });

  describe('pre remove middleware', () => {
    it('should not reparent/delete children when path is undefined', async () => {
      sweden.path = undefined;
      await sweden.remove();

      const locations = await Location.find({});
      const pathObject = locationsToPathObject(locations);

      pathObject.should.eql({
        Africa: '',
        Europe: '',
        Norway: '#eu#',
        Stockholm: '#eu#se#',
        Skansen: '#eu#se#sthlm#',
      });
    });

    describe('using onDelete="REPARENT" (default)', () => {
      it('should remove leaf nodes', async () => {
        await norway.remove();

        const locations = await Location.find({});
        const pathObject = locationsToPathObject(locations);

        pathObject.should.eql({
          Africa: '',
          Europe: '',
          Sweden: '#eu#',
          Stockholm: '#eu#se#',
          Skansen: '#eu#se#sthlm#',
        });
      });

      it('should reparent when higher level parent exist', async () => {
        await sweden.remove();
        const locations = await Location.find({});
        const pathObject = locationsToPathObject(locations);

        pathObject.should.eql({
          Africa: '',
          Europe: '',
          Norway: '#eu#',
          Stockholm: '#eu#',
          Skansen: '#eu#sthlm#',
        });
      });

      it('should reparent when higher level parent does not exist', async () => {
        await europe.remove();
        const locations = await Location.find({});
        const pathObject = locationsToPathObject(locations);

        pathObject.should.eql({
          Africa: '',
          Norway: '',
          Sweden: '',
          Stockholm: '#se#',
          Skansen: '#se#sthlm#',
        });
      });
    });

    describe('using onDelete="DELETE"', () => {
      before(async () => {
        // re-setup schema, model, database
        await mongoose.connection.close();

        LocationSchema = new mongoose.Schema({ _id: String, name: String });
        LocationSchema.plugin(MpathPlugin, {
          idType: String,
          pathSeparator: '#',
          onDelete: 'DELETE', // <- updated plugin option
        });

        dbConnection = await mongoose.connect('mongodb://localhost:27017/mongoose-path-tree', {
          connectTimeoutMS: 3000,
          keepAlive: 2000,
          reconnectTries: 30,
          useNewUrlParser: true,
          useUnifiedTopology: true,
        });

        try {
          Location = mongoose.model('Location', LocationSchema);
        } catch (ex) {
          mongoose.connection.deleteModel('Location');
          Location = mongoose.model('Location', LocationSchema);
        }
      });

      beforeEach(async () => await createLocations());
      afterEach(async () => await Location.deleteMany({}));

      it('should delete itself and all children', async () => {
        await sweden.remove();

        const locations = await Location.find({});
        const pathObject = locationsToPathObject(locations);

        pathObject.should.eql({
          Africa: '',
          Europe: '',
          Norway: '#eu#',
        });
      });
    });
  });

  describe('virtual field "level"', () => {
    it('should equal the number of ancestors', () => {
      africa.level.should.equal(1);
      europe.level.should.equal(1);
      norway.level.should.equal(2);
      sweden.level.should.equal(2);
      stockholm.level.should.equal(3);
      skansen.level.should.equal(4);
    });
  });

  describe('getImmediateChildren()', () => {
    it('using default params', async () => {
      const conditions = {};
      const fields = null;
      const options = {};

      const locations = await europe.getImmediateChildren(conditions, fields, options);

      locations.map(l => l.name).should.eql(['Norway', 'Sweden']);
    });

    it('using conditions (object)', async () => {
      const conditions = { name: 'Norway' };
      const fields = null;
      const options = {};

      const locations = await europe.getImmediateChildren(conditions, fields, options);

      locations.map(l => l.name).should.eql(['Norway']);
    });

    it('using conditions ($query)', async () => {
      const conditions = { $query: { name: 'Norway' } };
      const fields = null;
      const options = {};

      const locations = await europe.getImmediateChildren(conditions, fields, options);

      locations.map(l => l.name).should.eql(['Norway']);
    });

    it('using fields', async () => {
      const conditions = {};
      const fields = '_id';
      const options = { lean: true };

      const locations = await europe.getImmediateChildren(conditions, fields, options);
      locations.should.eql([{ _id: 'no' }, { _id: 'se' }]);
    });

    describe('using options (sort)', () => {
      it('ASC', async () => {
        const conditions = {};
        const fields = 'name';
        const options = {
          sort: { name: 1 },
          lean: true,
        };

        const locations = await europe.getImmediateChildren(conditions, fields, options);

        locations.should.eql([{ _id: 'no', name: 'Norway' }, { _id: 'se', name: 'Sweden' }]);
      });

      it('DESC', async () => {
        const conditions = {};
        const fields = 'name';
        const options = {
          sort: { name: -1 },
          lean: true,
        };

        const locations = await europe.getImmediateChildren(conditions, fields, options);

        locations.should.eql([{ _id: 'se', name: 'Sweden' }, { _id: 'no', name: 'Norway' }]);
      });
    });
  });

  describe('getAllChildren()', () => {
    it('using default params', async () => {
      const conditions = {};
      const fields = null;
      const options = {};

      const locations = await europe.getAllChildren(conditions, fields, options);

      locations.map(l => l.name).should.eql(['Norway', 'Sweden', 'Stockholm', 'Skansen']);
    });

    it('using conditions (object)', async () => {
      const conditions = { name: 'Stockholm' };
      const fields = null;
      const options = {};

      const locations = await europe.getAllChildren(conditions, fields, options);

      locations.map(l => l.name).should.eql(['Stockholm']);
    });

    it('using conditions ($query)', async () => {
      const conditions = { $query: { name: 'Stockholm' } };
      const fields = null;
      const options = {};

      const locations = await europe.getAllChildren(conditions, fields, options);

      locations.map(l => l.name).should.eql(['Stockholm']);
    });

    it('using fields', async () => {
      const conditions = {};
      const fields = '_id';
      const options = { lean: true };

      const locations = await europe.getAllChildren(conditions, fields, options);

      locations.should.eql([{ _id: 'no' }, { _id: 'se' }, { _id: 'sthlm' }, { _id: 'skansen' }]);
    });

    describe('using options (sort)', () => {
      it('ASC', async () => {
        const conditions = {};
        const fields = 'name';
        const options = {
          sort: { name: 1 },
          lean: true,
        };

        const locations = await europe.getAllChildren(conditions, fields, options);

        locations.should.eql([
          { _id: 'no', name: 'Norway' },
          { _id: 'skansen', name: 'Skansen' },
          { _id: 'sthlm', name: 'Stockholm' },
          { _id: 'se', name: 'Sweden' },
        ]);
      });

      it('DESC', async () => {
        const conditions = {};
        const fields = 'name';
        const options = {
          sort: { name: -1 },
          lean: true,
        };

        const locations = await europe.getAllChildren(conditions, fields, options);

        locations.should.eql([
          { _id: 'se', name: 'Sweden' },
          { _id: 'sthlm', name: 'Stockholm' },
          { _id: 'skansen', name: 'Skansen' },
          { _id: 'no', name: 'Norway' },
        ]);
      });
    });
  });

  describe('getParent()', () => {
    it('should get the parent', async () => {
      const fields = 'name';
      const options = { lean: true };

      const expectedParents = [
        [europe, null],
        [norway, { _id: 'eu', name: 'Europe' }],
        [sweden, { _id: 'eu', name: 'Europe' }],
        [stockholm, { _id: 'se', name: 'Sweden' }],
        [skansen, { _id: 'sthlm', name: 'Stockholm' }],
        [africa, null],
      ];

      Async.forEachSeries(expectedParents, (arr, asyncDone) => {
        const child = arr[0];
        const expectedParent = arr[1];

        child
          .getParent(fields, options, (error, parent) => {
            if (null === expectedParent) {
              should.not.exist(parent);
            } else {
              parent.should.eql(expectedParent);
            }
          })
          .then(() => asyncDone());
      });
    });
  });

  describe('getAncestors()', () => {
    it('using default params', async () => {
      const conditions = {};
      const fields = null;
      const options = {};

      const locations = await stockholm.getAncestors(conditions, fields, options);

      locations.map(l => l.name).should.eql(['Europe', 'Sweden']);
    });

    it('using conditions (plain object)', async () => {
      const conditions = { name: 'Europe' };
      const fields = null;
      const options = {};

      const locations = await stockholm.getAncestors(conditions, fields, options);

      locations.map(l => l.name).should.eql(['Europe']);
    });

    it('using conditions ($query)', async () => {
      const conditions = { $query: { name: 'Europe' } };
      const fields = null;
      const options = {};

      const locations = await stockholm.getAncestors(conditions, fields, options);

      locations.map(l => l.name).should.eql(['Europe']);
    });

    it('using fields', async () => {
      const conditions = {};
      const fields = '_id';
      const options = { lean: true };

      const locations = await stockholm.getAncestors(conditions, fields, options);

      locations.should.eql([{ _id: 'eu' }, { _id: 'se' }]);
    });

    it('using options (sort)', async () => {
      const conditions = {};
      const fields = '_id';
      const options = {
        sort: { name: -1 },
        lean: true,
      };

      const locations = await stockholm.getAncestors(conditions, fields, options);

      locations.should.eql([{ _id: 'se' }, { _id: 'eu' }]);
    });
  });

  describe('getChildrenTree()', () => {
    it('static method - args', async () => {
      const args = {
        fields: '_id name parent path',
        options: { lean: true },
      };

      const expectedTree = [
        {
          _id: 'af',
          name: 'Africa',
          path: '',
          children: [],
        },
        {
          _id: 'eu',
          name: 'Europe',
          path: '',
          children: [
            {
              _id: 'no',
              name: 'Norway',
              parent: 'eu',
              path: '#eu#',
              children: [],
            },
            {
              _id: 'se',
              name: 'Sweden',
              parent: 'eu',
              path: '#eu#',
              children: [
                {
                  _id: 'sthlm',
                  name: 'Stockholm',
                  parent: 'se',
                  path: '#eu#se#',
                  children: [
                    {
                      _id: 'skansen',
                      children: [],
                      name: 'Skansen',
                      parent: 'sthlm',
                      path: '#eu#se#sthlm#',
                    },
                  ],
                },
              ],
            },
          ],
        },
      ];

      const locationTree = await Location.getChildrenTree(args);
      locationTree.should.eql(expectedTree);
    });

    it('includes path and parent fields', async () => {
      const args = {
        fields: '_id name',
        options: { lean: true },
      };

      const expectedTree = [
        {
          _id: 'sthlm',
          children: [
            {
              _id: 'skansen',
              children: [],
              name: 'Skansen',
              parent: 'sthlm',
              path: '#eu#se#sthlm#',
            },
          ],
          name: 'Stockholm',
          parent: 'se',
          path: '#eu#se#',
        },
      ];

      const locationTree = await sweden.getChildrenTree(args);
      locationTree.should.eql(expectedTree);
    });

    it('fields as object', async () => {
      const args = {
        fields: { _id: 1, name: 1 },
        options: { lean: true },
      };

      const expectedTree = [
        {
          _id: 'sthlm',
          children: [
            {
              _id: 'skansen',
              children: [],
              name: 'Skansen',
              parent: 'sthlm',
              path: '#eu#se#sthlm#',
            },
          ],
          name: 'Stockholm',
          parent: 'se',
          path: '#eu#se#',
        },
      ];

      const locationTree = await sweden.getChildrenTree(args);
      locationTree.should.eql(expectedTree);
    });

    it('options.lean=false should return Mongoose Documents', async () => {
      const args = { fields: { _id: 1, name: 1 }, options: { lean: false } };

      return sweden.getChildrenTree(args).then(tree => {
        tree[0].name.should.eql('Stockholm');

        tree[0].getChildrenTree(args).then(subtree => {
          subtree[0].name.should.eql('Skansen');
        });
      });
    });

    it('should filter by minLevel', async () => {
      const args = {
        fields: { _id: 1, name: 1 },
        options: { lean: true },
        minLevel: 3,
      };
      const tree = await Location.getChildrenTree(args);

      tree.should.eql([
        {
          _id: 'sthlm',
          name: 'Stockholm',
          parent: 'se',
          path: '#eu#se#',
          children: [
            {
              _id: 'skansen',
              children: [],
              name: 'Skansen',
              parent: 'sthlm',
              path: '#eu#se#sthlm#',
            },
          ],
        },
      ]);
    });

    describe('should sort', () => {
      it('ASC', async () => {
        const args = {
          filters: { parent: 'eu' },
          fields: { _id: 1, name: 1 },
          options: {
            lean: true,
            sort: { name: 1 },
          },
        };
        const tree = await Location.getChildrenTree(args);

        tree.should.eql([
          {
            _id: 'no',
            name: 'Norway',
            parent: 'eu',
            path: '#eu#',
            children: [],
          },
          {
            _id: 'se',
            name: 'Sweden',
            parent: 'eu',
            path: '#eu#',
            children: [],
          },
        ]);
      });

      it('DESC', async () => {
        const args = {
          filters: { parent: 'eu' },
          fields: { _id: 1, name: 1 },
          options: {
            lean: true,
            sort: { name: -1 },
          },
        };
        const tree = await Location.getChildrenTree(args);

        tree.should.eql([
          {
            _id: 'se',
            name: 'Sweden',
            parent: 'eu',
            path: '#eu#',
            children: [],
          },
          {
            _id: 'no',
            name: 'Norway',
            parent: 'eu',
            path: '#eu#',
            children: [],
          },
        ]);
      });
    });
  });

  describe('util', () => {
    it('should get level', function() {
      let level;

      level = MpathPlugin.util.getLevelByPathAndSeparator('', '#');
      level.should.equal(1);

      level = MpathPlugin.util.getLevelByPathAndSeparator('#foo#', '#');
      level.should.equal(2);

      level = MpathPlugin.util.getLevelByPathAndSeparator('#foo#bar#', '#');
      level.should.equal(3);
    });

    describe('should createTree', () => {
      it('default options', () => {
        const nodes = [
          { _id: 'eu', name: 'Europe', parent: '', path: '' },
          { _id: 'no', name: 'Norway', parent: 'eu', path: '#eu#' },
          { _id: 'se', name: 'Sweden', parent: 'eu', path: '#eu#' },
          {
            _id: 'sthlm',
            name: 'Stockholm',
            parent: 'se',
            path: '#eu#se#',
          },
          {
            _id: 'skansen',
            name: 'Skansen',
            parent: 'sthlm',
            path: '#eu#se#sthlm#',
          },
        ];

        const expectedTree = [
          {
            _id: 'eu',
            name: 'Europe',
            parent: '',
            path: '',
            children: [
              {
                _id: 'no',
                name: 'Norway',
                parent: 'eu',
                path: '#eu#',
                children: [],
              },
              {
                _id: 'se',
                name: 'Sweden',
                parent: 'eu',
                path: '#eu#',
                children: [
                  {
                    _id: 'sthlm',
                    name: 'Stockholm',
                    parent: 'se',
                    path: '#eu#se#',
                    children: [
                      {
                        _id: 'skansen',
                        name: 'Skansen',
                        parent: 'sthlm',
                        path: '#eu#se#sthlm#',
                        children: [],
                      },
                    ],
                  },
                ],
              },
            ],
          },
        ];

        const result = MpathPlugin.util.listToTree(nodes);
        result.should.eql(expectedTree);
      });
    });
  });
});
